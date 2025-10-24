#!/usr/bin/env python3
"""
Analyze py-spy flamegraph SVG to extract hot functions
"""

import sys
import re
from collections import defaultdict
from pathlib import Path


def parse_flamegraph(svg_path):
    """Extract function timing data from py-spy SVG flamegraph"""

    with open(svg_path, 'r') as f:
        svg_content = f.read()

    # Find all <g> elements with function names and widths
    # Format: <g class="func_g" onmouseover="s('...')" ... >
    #         <title>function_name (N samples, X.XX%)</title>
    #         <rect ... width="..." ... />

    pattern = r'<title>([^<]+) \((\d+) samples[^)]*\)</title>'
    matches = re.findall(pattern, svg_content)

    functions = defaultdict(int)
    total_samples = 0

    for func_name, samples in matches:
        samples = int(samples)
        functions[func_name] += samples
        total_samples = max(total_samples, samples)

    return functions, total_samples


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/analyze_flamegraph.py profile_ingestion_*.svg")
        sys.exit(1)

    svg_path = sys.argv[1]

    if not Path(svg_path).exists():
        print(f"❌ File not found: {svg_path}")
        sys.exit(1)

    print("🔥 Analyzing Flamegraph...")
    print("=" * 80)

    functions, total_samples = parse_flamegraph(svg_path)

    # Sort by samples (descending)
    sorted_funcs = sorted(functions.items(), key=lambda x: x[1], reverse=True)

    # Filter for Arc-related functions and hot paths
    print("\n📊 Top Functions by CPU Time:\n")

    shown = 0
    for func_name, samples in sorted_funcs:
        if shown >= 30:  # Show top 30
            break

        # Calculate percentage (rough estimate)
        percent = (samples / total_samples * 100) if total_samples > 0 else 0

        # Show if significant time (>0.5%) or Arc-related
        if percent > 0.5 or any(keyword in func_name.lower() for keyword in
                                ['arrow_writer', 'msgpack', 'ingest', 'write', 'flush', 'lock']):
            bar_width = int(percent * 0.5)  # Scale to 50 chars max
            bar = "█" * bar_width

            print(f"{percent:5.1f}% {bar:50s} {func_name}")
            shown += 1

    print("\n" + "=" * 80)
    print("\n🎯 Key Areas to Investigate:\n")

    # Look for specific bottlenecks
    arc_functions = {k: v for k, v in functions.items() if any(
        keyword in k.lower() for keyword in
        ['arrow_writer', 'msgpack', 'ingest', 'api/', 'write', 'flush']
    )}

    if arc_functions:
        print("Arc-specific functions (these are where we can optimize):\n")
        for func_name, samples in sorted(arc_functions.items(), key=lambda x: x[1], reverse=True)[:10]:
            percent = (samples / total_samples * 100) if total_samples > 0 else 0
            print(f"  • {percent:5.1f}% - {func_name}")

    # Check for lock contention
    lock_functions = {k: v for k, v in functions.items() if 'lock' in k.lower()}
    if lock_functions:
        total_lock_time = sum(lock_functions.values())
        lock_percent = (total_lock_time / total_samples * 100) if total_samples > 0 else 0
        print(f"\n⚠️  Lock-related operations: {lock_percent:.1f}% of CPU time")
        if lock_percent > 5:
            print("   → Significant lock contention detected!")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Quick profiling helper for Arc ingestion pipeline

This script helps you profile Arc's ingestion performance using py-spy.

Usage:
    1. Start Arc: ./start.sh native
    2. In another terminal: python scripts/profile_arc_ingestion.py
    3. Run your benchmark
    4. Open the generated flamegraph

The script will:
- Find the Arc worker process
- Start py-spy profiling
- Wait for you to run the benchmark
- Generate a flamegraph showing where time is spent
"""

import subprocess
import sys
import time
import signal
import os


def find_arc_worker():
    """Find Arc worker process PID"""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "gunicorn.*api.main:app"],
            capture_output=True,
            text=True,
            timeout=5
        )
        pids = [pid for pid in result.stdout.strip().split('\n') if pid]

        if not pids:
            print("❌ No Arc worker process found")
            print("   Make sure Arc is running: ./start.sh native")
            return None

        # Use first worker PID
        pid = int(pids[0])
        print(f"✅ Found Arc worker PID: {pid}")
        return pid

    except Exception as e:
        print(f"❌ Error finding Arc process: {e}")
        return None


def profile_ingestion(pid, duration=60):
    """Profile Arc ingestion with py-spy"""

    profile_file = f"profile_ingestion_{int(time.time())}.svg"

    print(f"\n🔥 Starting py-spy profiler...")
    print(f"   Duration: {duration} seconds")
    print(f"   Output: {profile_file}")
    print(f"\n📊 Now run your benchmark in another terminal:")
    print(f"   python scripts/benchmark_ingestion.py")
    print(f"\n⏳ Profiling...")

    try:
        # Build py-spy command (--native not supported on macOS)
        pyspy_cmd = [
            "./venv/bin/py-spy",
            "record",
            "-o", profile_file,
            "-d", str(duration),
            "-p", str(pid),
            "--rate", "100",  # 100 samples/sec for good detail
            "--subprocesses"
        ]

        subprocess.run(pyspy_cmd, check=True)

        print(f"\n✅ Profiling complete!")
        print(f"   Flamegraph: {profile_file}")
        print(f"   View with: open {profile_file}")

        return profile_file

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Profiling failed: {e}")
        print("   Make sure py-spy is installed: ./venv/bin/pip install py-spy")
        return None
    except KeyboardInterrupt:
        print(f"\n⚠️  Profiling interrupted")
        return None


def main():
    print("🔍 Arc Ingestion Profiler")
    print("=" * 60)

    # Check if py-spy is available
    if not os.path.exists("./venv/bin/py-spy"):
        print("❌ py-spy not found")
        print("   Installing py-spy...")
        subprocess.run(["./venv/bin/pip", "install", "py-spy"], check=True)
        print("✅ py-spy installed")

    # Find Arc worker
    pid = find_arc_worker()
    if not pid:
        sys.exit(1)

    # Get profiling duration
    duration = 60  # Default 60 seconds
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
        except ValueError:
            print(f"⚠️  Invalid duration '{sys.argv[1]}', using default: {duration}s")

    # Profile
    profile_file = profile_ingestion(pid, duration)

    if profile_file:
        print("\n📝 How to interpret the flamegraph:")
        print("   • Width = CPU time spent")
        print("   • Look for wide bars = hot paths")
        print("   • Follow call stacks from bottom to top")
        print("   • Focus on functions inside 'arc' code, not external libraries")
        print("\n🎯 What to look for:")
        print("   • msgpack_decoder.decode() - MessagePack decoding time")
        print("   • arrow_writer.write() - Buffer operations")
        print("   • arrow_writer._flush_records() - Parquet writing")
        print("   • Lock acquisition/contention")
        print("   • Dictionary operations")


if __name__ == "__main__":
    main()

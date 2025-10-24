#!/usr/bin/env python3
"""
Profile Arc ingestion pipeline

Usage:
    python scripts/profile_ingestion.py --mode [cprofile|pyspy|memory]
"""

import argparse
import subprocess
import sys
import time
import signal
import os


def profile_with_pyspy(duration=30):
    """Profile with py-spy (low overhead, production-safe)"""
    print("🔍 Starting Arc server for py-spy profiling...")

    # Start Arc in background
    arc_process = subprocess.Popen(
        ["./start.sh", "native"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid
    )

    # Wait for server to start
    print("⏳ Waiting for server to start...")
    time.sleep(15)

    # Get Arc PID
    try:
        result = subprocess.run(
            ["pgrep", "-f", "gunicorn.*api.main:app"],
            capture_output=True,
            text=True
        )
        pids = result.stdout.strip().split('\n')
        if not pids or pids[0] == '':
            print("❌ Failed to find Arc process")
            os.killpg(os.getpgid(arc_process.pid), signal.SIGTERM)
            return

        # Use first worker PID
        arc_pid = int(pids[0])
        print(f"✅ Found Arc PID: {arc_pid}")

        # Start py-spy profiling
        print(f"🔥 Profiling for {duration} seconds with py-spy...")
        profile_file = f"profile_pyspy_{int(time.time())}.svg"

        pyspy_cmd = [
            "./venv/bin/py-spy",
            "record",
            "-o", profile_file,
            "-d", str(duration),
            "-p", str(arc_pid),
            "--rate", "100",  # 100 samples/sec
            "--subprocesses"
        ]

        subprocess.run(pyspy_cmd)

        print(f"\n✅ Profile saved to: {profile_file}")
        print(f"   Open with: open {profile_file}")

    finally:
        # Kill Arc
        print("\n🛑 Stopping Arc server...")
        os.killpg(os.getpgid(arc_process.pid), signal.SIGTERM)
        time.sleep(2)


def profile_with_cprofile():
    """Profile with cProfile (detailed, higher overhead)"""
    print("🔍 Starting cProfile profiling...")
    print("⚠️  Note: This requires running Arc with cProfile enabled in code")
    print("\nAdd to api/main.py startup:")
    print("""
import cProfile
import pstats
import atexit

profiler = cProfile.Profile()
profiler.enable()

def save_profile():
    profiler.disable()
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.dump_stats('profile.stats')
    print("Profile saved to profile.stats")

atexit.register(save_profile)
    """)


def profile_memory():
    """Profile memory usage"""
    print("🔍 Starting memory profiling...")
    print("⚠️  Note: This requires running Arc with memory profiling enabled")
    print("\nAdd to api/main.py startup:")
    print("""
import tracemalloc
import atexit

tracemalloc.start()

def print_memory_stats():
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')

    print("\\n=== Top 20 Memory Allocations ===")
    for stat in top_stats[:20]:
        print(stat)

atexit.register(print_memory_stats)
    """)


def main():
    parser = argparse.ArgumentParser(description="Profile Arc ingestion pipeline")
    parser.add_argument(
        "--mode",
        choices=["pyspy", "cprofile", "memory"],
        default="pyspy",
        help="Profiling mode (default: pyspy)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Profiling duration in seconds (default: 30)"
    )

    args = parser.parse_args()

    if args.mode == "pyspy":
        profile_with_pyspy(args.duration)
    elif args.mode == "cprofile":
        profile_with_cprofile()
    elif args.mode == "memory":
        profile_memory()


if __name__ == "__main__":
    main()

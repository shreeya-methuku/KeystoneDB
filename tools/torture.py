#!/usr/bin/env python3
"""Kill-9 torture test for KeystoneDB WAL durability."""

import os
import random
import signal
import subprocess
import sys
import tempfile
import time


def main():
    iterations = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    torture_bin = os.path.join(project_dir, "build", "torture")

    if not os.path.isfile(torture_bin):
        print(f"ERROR: {torture_bin} not found. Build first.", file=sys.stderr)
        sys.exit(1)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_dir = os.path.join(tmpdir, "db")
        prev_prefix = 0

        for i in range(1, iterations + 1):
            proc = subprocess.Popen([torture_bin, "write", db_dir])
            time.sleep(random.uniform(0.02, 0.25))
            proc.send_signal(signal.SIGKILL)
            proc.wait()

            result = subprocess.run(
                [torture_bin, "verify", db_dir],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                print(f"FAIL at iteration {i}: {result.stderr}", file=sys.stderr)
                sys.exit(1)

            prefix_len = int(result.stdout.strip())
            if prefix_len < prev_prefix:
                print(
                    f"FAIL at iteration {i}: prefix shrank {prev_prefix} -> {prefix_len}",
                    file=sys.stderr,
                )
                sys.exit(1)

            prev_prefix = prefix_len

            if i % 10 == 0:
                print(f"iteration {i}/{iterations}: prefix={prefix_len}")

        print(f"PASS — {iterations} iterations, final prefix={prev_prefix}")


if __name__ == "__main__":
    main()

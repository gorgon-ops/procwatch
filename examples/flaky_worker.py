"""A worker that hangs on purpose.

    python examples/flaky_worker.py --input data/north   # unwatched, runs as-is
    procwatch run -c examples/pipeline.toml              # hang detected, restarted
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path

from procwatch import Heartbeat


def process_file(path: Path) -> None:
    time.sleep(random.uniform(0.2, 0.6))


def deadlock() -> None:
    """Still holds its memory and handles and looks healthy in the process list; only
    the missing heartbeats give it away.
    """
    print("worker: entering the stuck state", flush=True)
    while True:
        time.sleep(3600)


def main() -> int:
    parser = argparse.ArgumentParser(description="A deliberately unreliable worker.")
    parser.add_argument("--input", required=True, help="Directory to process.")
    parser.add_argument(
        "--hang-after",
        type=int,
        default=5,
        help="Hang after this many files (0 disables the hang).",
    )
    args = parser.parse_args()

    directory = Path(args.input)
    if not directory.is_dir():
        print(f"worker: no such directory: {directory}", file=sys.stderr)
        return 1

    # Inert unless procwatch launched us.
    with Heartbeat.from_env() as heartbeat:
        files = sorted(p for p in directory.iterdir() if p.is_file())
        print(f"worker: {len(files)} file(s) in {directory}", flush=True)

        for index, path in enumerate(files, start=1):
            if args.hang_after and index > args.hang_after:
                deadlock()

            process_file(path)
            heartbeat.progress(
                done=index, total=len(files), current=path.name
            )
            print(f"worker: [{index}/{len(files)}] {path.name}", flush=True)

        print("worker: finished", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())

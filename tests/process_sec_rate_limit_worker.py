"""Independent process used by the SEC request-rate-limit test."""

from __future__ import annotations

import sys
import time
from pathlib import Path

from finresearch.providers.sec import _SECRequestRateLimiter


def main() -> int:
    """Wait on a shared limiter and record the granted request-start time."""
    lock_path = Path(sys.argv[1])
    coordination_directory = Path(sys.argv[2])
    worker_id = sys.argv[3]
    (coordination_directory / f"ready-{worker_id}").touch()
    release_path = coordination_directory / "go"
    deadline = time.monotonic() + 10
    while not release_path.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("SEC rate-limit barrier timed out")
        time.sleep(0.01)

    _SECRequestRateLimiter(lock_path=lock_path).wait()
    (coordination_directory / f"request-{worker_id}").write_text(
        str(time.monotonic_ns()),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

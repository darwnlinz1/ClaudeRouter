"""Command-line entry point for the local orchestrator service."""
from __future__ import annotations

import argparse
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()
    # Only this canonical service process may reconcile stale active snapshots.
    # Child pytest/TestClient processes inherit the value but have a different
    # PID, so importing ``server:app`` there cannot interrupt live tasks.
    os.environ["ORCH_RECOVERY_OWNER_PID"] = str(os.getpid())
    uvicorn.run(
        "server:app",
        host="127.0.0.1",
        port=args.port,
        reload=args.reload,
        proxy_headers=False,
        forwarded_allow_ips="",
    )


if __name__ == "__main__":
    main()

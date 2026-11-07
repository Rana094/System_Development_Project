"""Trusted read-only child entrypoint; no mounts, publication or job imports."""

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hybrid_storage.health import ProbeRequest, verify_request


def main():
    if len(sys.argv) != 2 or len(sys.argv[1].encode()) > 8192:
        return 2
    try:
        request = ProbeRequest(**json.loads(sys.argv[1]))
        result = verify_request(request)
    except (ValueError, TypeError):
        return 2
    print(json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

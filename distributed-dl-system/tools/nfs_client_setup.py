"""Plan selected NFS clients (all three if unspecified); live actions need approval."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hybrid_storage.nfs_client import main

if __name__ == "__main__":
    raise SystemExit(main())

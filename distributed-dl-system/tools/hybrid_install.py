"""Explicit Phase14.2 toolkit; preparation is offline, host mutations need approval."""
from pathlib import Path
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if __name__ == '__main__':
    from hybrid_storage.deployment_toolkit import main
    raise SystemExit(main())

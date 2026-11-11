"""Explicit reviewed offline pilot setup; defaults to a no-write dry run."""
from pathlib import Path
import sys
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
if __name__=='__main__':
    from hybrid_storage.pilot_provision import main
    main()

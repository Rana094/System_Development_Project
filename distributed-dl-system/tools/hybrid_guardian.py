"""Root-owned systemd entry point. Never accepts student command text."""
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/'agent'))
from hybrid_storage.linux_guardian import main

if __name__=='__main__':
    if len(sys.argv)!=2:raise SystemExit('Exactly one protected launch record required')
    raise SystemExit(main(sys.argv[1]))

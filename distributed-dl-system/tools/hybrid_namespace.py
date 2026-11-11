"""Private trusted namespace setup; invoked only by root guardian with a memfd."""
from pathlib import Path
import sys
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
if __name__=='__main__':
    if len(sys.argv)!=2:raise SystemExit('One inherited guardian manifest fd required')
    from hybrid_storage.linux_namespace import main
    main(int(sys.argv[1]))

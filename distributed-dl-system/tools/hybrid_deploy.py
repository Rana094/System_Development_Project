"""Phase 14.2 offline delivery plan/verification; apply always refuses."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hybrid_storage.delivery_plan import main

if __name__ == '__main__':
    raise SystemExit(main())

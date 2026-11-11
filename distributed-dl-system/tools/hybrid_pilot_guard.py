"""Reviewed worker boot preflight; creates only the owned missing nft table."""
from pathlib import Path
import sys
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
if __name__=='__main__':
    from hybrid_storage.linux_boundary import protected_json,LinuxConfig,LinuxBoundary
    from hybrid_storage.pilot_provision import firewall
    config=protected_json(Path(sys.argv[1]))
    firewall(config)
    LinuxBoundary(LinuxConfig(**config['boundary'])).preflight()

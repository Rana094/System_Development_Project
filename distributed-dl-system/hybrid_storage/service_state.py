"""Opt-in LOCAL service state, validated before legacy modules create files."""
import os
from pathlib import Path


def controller_state(default):
    value=os.environ.get('DL_CONTROLLER_STATE_ROOT')
    if value is None:return Path(default)
    from .backends import _root,_directory
    from .worker_cache import _protected
    root=_root(Path(value))
    with _directory(root) as fd:_protected(fd,directory=True)
    return root


def checkpoint_state(default):
    return controller_state(default)/'legacy-checkpoints' if os.environ.get('DL_CONTROLLER_STATE_ROOT') else Path(default)

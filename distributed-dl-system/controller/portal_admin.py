"""Explicit local-operator admin grants for the existing portal; fail closed."""
import fcntl
import json
import os
from pathlib import Path
import tempfile

from hybrid_storage.admin_observations import private_json
from hybrid_storage.backends import _directory
from job_spec import validate_owner


def admins(directory):
    value=private_json(directory,'admins.json',maximum=16*1024)
    if type(value) is not list or len(value)>256:raise ValueError('Invalid admin allowlist')
    for owner in value:validate_owner(owner)
    return set(value)


def is_admin(directory,owner):
    if not owner:return False
    try:return owner in admins(directory)
    except (OSError,ValueError,TypeError):return False


def set_admin(directory,owner,*,enabled):
    """CLI only; grant existing login, no public role-assignment API."""
    validate_owner(owner);directory=Path(directory)
    if type(enabled) is not bool:raise ValueError('Expected boolean grant')
    users=private_json(directory,'users.json',maximum=1024*1024)
    if type(users) is not dict or owner not in users:raise ValueError('Create the login before granting admin access')
    with (directory/'.users.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        try:current=admins(directory)
        except FileNotFoundError:current=set()
        if enabled:current.add(owner)
        else:current.discard(owner)
        if len(current)>256:raise ValueError('Admin allowlist limit exceeded')
        fd,name=tempfile.mkstemp(prefix='.admins-',dir=directory)
        try:
            with os.fdopen(fd,'w') as stream:
                json.dump(sorted(current),stream);stream.write('\n');stream.flush();os.fsync(stream.fileno())
            os.replace(name,directory/'admins.json')
            with _directory(directory) as parent:os.fsync(parent)
        finally:Path(name).unlink(missing_ok=True)

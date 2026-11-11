"""Recovery-point tests use NEW local SQLite files only; never import controller."""
from contextlib import closing
import errno
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from hybrid_storage import state_backup as backup


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name);self.source=self.base/'jobs.sqlite';self.output=self.base/'recovery'
        self.db=sqlite3.connect(self.source);self.addCleanup(self.db.close)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('CREATE TABLE jobs(id INTEGER PRIMARY KEY, value BLOB, score REAL, note TEXT)')
        self.db.execute('INSERT INTO jobs VALUES(?,?,?,?)',(1,b'\0private-secret',1.25,'unicode λ\0'))
        self.db.execute('PRAGMA user_version=81');self.db.commit()

    def run_backup(self,**limits):return backup.backup(self.source,self.output,**limits)

    def test_live_wal_backup_and_separate_restore_preserve_source_and_sensitive_rows(self):
        main=self.source.read_bytes();wal=Path(str(self.source)+'-wal').read_bytes();rows=list(self.db.iterdump())
        result=self.run_backup()
        self.assertEqual(self.source.read_bytes(),main);self.assertEqual(Path(str(self.source)+'-wal').read_bytes(),wal)
        self.assertEqual(list(self.db.iterdump()),rows)
        self.assertEqual(result['user_version'],81);self.assertEqual(result['table_counts']['jobs'],1)
        self.assertTrue(result['restored_copy_verified']);self.assertFalse(result['coordinated_filesystem_backup'])
        self.assertFalse(result['off_host_redundancy']);self.assertNotIn('private-secret',json.dumps(result))
        self.assertEqual(self.output.stat().st_mode&0o777,0o700)
        for name in ('snapshot.sqlite','restored.sqlite','backup.json'):
            self.assertEqual((self.output/name).stat().st_mode&0o777,0o600)
        with closing(sqlite3.connect(self.output/'restored.sqlite')) as restored:
            self.assertEqual(restored.execute('SELECT * FROM jobs').fetchall(),self.db.execute('SELECT * FROM jobs').fetchall())

    def test_existing_destination_never_overwritten_and_no_implicit_live_restore(self):
        self.run_backup();files={p.name:p.read_bytes() for p in self.output.iterdir()}
        with self.assertRaises(FileExistsError):self.run_backup()
        self.assertEqual({p.name:p.read_bytes() for p in self.output.iterdir()},files)
        self.assertFalse(hasattr(backup,'restore_live'))

    def test_symlink_hardlink_and_symlinked_parent_refused(self):
        alias=self.base/'alias.sqlite';alias.symlink_to(self.source)
        with self.assertRaises(OSError):backup.backup(alias,self.output)
        alias.unlink();os.link(self.source,alias)
        with self.assertRaises(ValueError):self.run_backup()
        alias.unlink();parent=self.base/'alias';parent.symlink_to(self.base,target_is_directory=True)
        with self.assertRaises(OSError):backup.backup(self.source,parent/'recovery')
        self.assertFalse(self.output.exists())

    def test_nonlocal_source_or_destination_never_admitted(self):
        with patch.object(backup,'require_local_fd',side_effect=OSError(errno.EXDEV,'NFS denied')):
            with self.assertRaises(OSError):self.run_backup()
        self.assertFalse(self.output.exists())

    def test_byte_and_free_space_limits_refuse_before_output_creation(self):
        with self.assertRaises(OSError):self.run_backup(max_bytes=1)
        self.assertFalse(self.output.exists())
        with self.assertRaises(OSError):self.run_backup(reserve_bytes=2**63)
        self.assertFalse(self.output.exists())

    def test_missing_source_is_not_created(self):
        missing=self.base/'missing.sqlite'
        with self.assertRaises(FileNotFoundError):backup.backup(missing,self.output)
        self.assertFalse(missing.exists());self.assertFalse(self.output.exists())

    def test_deadline_abort_retains_new_incomplete_output_without_completion_marker(self):
        with patch.object(backup.time,'monotonic',side_effect=[0,200]):
            with self.assertRaises(TimeoutError):self.run_backup(timeout_seconds=1)
        self.assertFalse((self.output/'backup.json').exists())
        self.assertEqual(self.db.execute('SELECT count(*) FROM jobs').fetchone()[0],1)

    def test_foreign_key_failure_is_not_called_verified_and_source_unchanged(self):
        self.db.execute('CREATE TABLE parent(id INTEGER PRIMARY KEY)')
        self.db.execute('CREATE TABLE child(id INTEGER REFERENCES parent(id))')
        self.db.execute('INSERT INTO child VALUES(42)');self.db.commit()
        before=list(self.db.iterdump())
        with self.assertRaisesRegex(ValueError,'foreign_key_check'):self.run_backup()
        self.assertFalse((self.output/'backup.json').exists());self.assertEqual(list(self.db.iterdump()),before)

    def test_restored_data_mismatch_prevents_manifest_without_touching_source(self):
        real=backup._fingerprint;count=0
        def mismatch(*args):
            nonlocal count
            count+=1;result=real(*args)
            if count==2:result['logical_sha256']='0'*64
            return result
        with patch.object(backup,'_fingerprint',side_effect=mismatch):
            with self.assertRaisesRegex(ValueError,'differs'):self.run_backup()
        self.assertFalse((self.output/'backup.json').exists())
        self.assertEqual(self.db.execute('SELECT count(*) FROM jobs').fetchone()[0],1)

    def test_all_source_connections_are_readonly_and_closed(self):
        original=backup._connect;calls=[];handles=[]
        def record(path,mode):
            calls.append((path,mode));db=original(path,mode);handles.append(db);return db
        with patch.object(backup,'_connect',side_effect=record):self.run_backup()
        self.assertTrue(all(mode=='ro' for path,mode in calls if path==self.source))
        for db in handles:
            with self.assertRaises(sqlite3.ProgrammingError):db.execute('SELECT 1')


if __name__=='__main__':unittest.main(verbosity=2)

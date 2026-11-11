"""Protected probe tests use controlled fake files; no root, SSH or live NFS."""
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from hybrid_storage import readiness_probe as probe


class ProbeTests(unittest.TestCase):
    def test_unprivileged_call_cannot_request_protected_operations(self):
        with patch.object(probe.os,'geteuid',return_value=1000):
            with self.assertRaises(PermissionError):probe.probe('controller',privileged=True)
        with self.assertRaises(PermissionError):probe.probe('worker',source='192.168.10.214:/srv/ml-shared/objects',verify_test=True)

    def test_controller_root_reads_only_fixed_metadata_and_never_runs_commands(self):
        with patch.object(probe.os,'geteuid',return_value=0),patch.object(probe,'_text',return_value='metadata') as text, \
             patch.object(probe,'_metadata',return_value={'contents_read':False}), \
             patch('subprocess.run',side_effect=AssertionError('no commands')), \
             patch('os.write',side_effect=AssertionError('no writes')):
            result=probe.probe('controller',privileged=True)
        self.assertEqual({c.args[0] for c in text.call_args_list},{'/etc/exports','/etc/exports.d/distributed-ml.exports','/var/lib/nfs/etab','/proc/fs/nfsd/versions'})
        self.assertFalse(result['writes_or_config_changes'])

    def test_absent_mount_never_reads_ordinary_underlying_directory(self):
        with patch.object(probe.os,'geteuid',return_value=0),patch.object(probe,'_text',return_value=''), \
             patch.object(probe,'_metadata',return_value={}),patch.object(probe,'_test_object',side_effect=AssertionError('no fallback')):
            result=probe.probe('worker',source='192.168.10.214:/srv/ml-shared/objects',privileged=True,verify_test=True)
        self.assertFalse(result['mount_admission']['ok']);self.assertFalse(result['public_test_object']['ok'])

    def test_test_object_writes_zero_bytes_and_only_erofs_proves_ro(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'test';data=b'public test';path.write_bytes(data)
            normal=probe._open
            def opened(p,flags):
                if flags==os.O_WRONLY:raise OSError(errno.EROFS,'readonly')
                return normal(p,flags)
            with patch.object(probe,'TEST_SHA',hashlib.sha256(data).hexdigest()),patch.object(probe,'TEST_BYTES',len(data)), \
                 patch.object(probe,'_open',side_effect=opened),patch('os.write',side_effect=AssertionError('no writes')):
                result=probe._test_object(path,test_write=True)
            self.assertTrue(result['read_only_write_denial_verified']);self.assertEqual(result['bytes_written'],0)
            for error in (errno.EACCES,None):
                def writable_or_denied(p,flags):
                    if flags==os.O_WRONLY and error:raise OSError(error,'permission')
                    return normal(p,flags)
                with patch.object(probe,'TEST_SHA',hashlib.sha256(data).hexdigest()),patch.object(probe,'TEST_BYTES',len(data)),patch.object(probe,'_open',side_effect=writable_or_denied):
                    result=probe._test_object(path,test_write=True)
                self.assertFalse(result['read_only_write_denial_verified'])
                self.assertEqual(path.read_bytes(),data)

    def test_links_corruption_and_oversize_metadata_refuse(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'test';path.write_bytes(b'x'*165)
            with self.assertRaisesRegex(ValueError,'hash mismatch'):probe._test_object(path,test_write=False)
            alias=Path(temp)/'alias';alias.symlink_to(path)
            with self.assertRaises(OSError):probe._text(alias)
            with self.assertRaises(ValueError):probe._text(path,limit=1)

    def test_portable_program_is_standalone_and_no_controller_sqlite_or_network_import_effect(self):
        spec=importlib.util.spec_from_file_location('readiness_cli',ROOT/'tools/storage_readiness.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        code=module.worker_program()+"\nassert 'sqlite3' not in sys.modules and 'job_store' not in sys.modules\n"
        # Invalid mount source fails without touching any protected path or network.
        run=subprocess.run([sys.executable,'-I','-B','-','--role','worker','--source','127.0.0.1:/srv/ml-shared/objects'],input=code,text=True,capture_output=True,timeout=10)
        self.assertNotEqual(run.returncode,0);self.assertIn('Unsafe controller input source',run.stderr)

    def test_portable_valid_source_reads_numeric_proc_pid_without_following_proc_self(self):
        spec=importlib.util.spec_from_file_location('readiness_cli_pid',ROOT/'tools/storage_readiness.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        code=module.worker_program()+"\nassert 'sqlite3' not in sys.modules and 'job_store' not in sys.modules\n"
        run=subprocess.run([sys.executable,'-I','-B','-','--role','worker','--source','192.168.10.214:/srv/ml-shared/objects'],input=code,text=True,capture_output=True,timeout=10)
        self.assertEqual(run.returncode,0,run.stderr)
        data=json.loads(run.stdout);self.assertIn('mount_admission',data)
        self.assertNotIn('NotADirectoryError',run.stderr)


if __name__=='__main__':unittest.main(verbosity=2)

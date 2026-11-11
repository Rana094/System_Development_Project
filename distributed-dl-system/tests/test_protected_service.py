"""No root/network/systemd mutations. Disposable result and service contracts."""
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from hybrid_storage.protected_results import unpack
from hybrid_storage.service_entry import configuration,configure_environment
from hybrid_storage.linux_boundary import atomic_json

class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();self.addCleanup(self.t.cleanup)
        self.root=Path(self.t.name);self.root.chmod(0o700)
        self.cfg=dict(schema_version=1,role='worker',state_root=str(self.root),code_root=str(ROOT),bind_ip='192.168.10.150',port=5001,
            settings=dict(storage_backend='nfs',worker_cache_enabled=True,storage_aware_staging=True,scheduler_policy='resource'),
            nodes=[dict(id='pi02',ip='192.168.10.150',port=5001)],credentials=dict(pi02=str(self.root/'key.json')),
            paths=dict(worker_id='pi02'),boundary={},capsule=str(self.root/'capsule.json'))
    def save(self):
        p=self.root/'config.json';atomic_json(p,self.cfg);return p
    def test_explicit_profile_load_does_not_import_legacy_startup(self):
        with patch('sqlite3.connect',side_effect=AssertionError('No DB on config read')):
            self.assertEqual(configuration(self.save()),self.cfg)
    def test_flags_source_scope_and_endpoint_fail_closed(self):
        for key,value in [('code_root','/tmp/unreviewed'),('bind_ip','100.91.135.18'),('role','shell'),('credentials',{})]:
            old=self.cfg[key];self.cfg[key]=value
            with self.assertRaises((ValueError,PermissionError)):configuration(self.save())
            self.cfg[key]=old
    def test_configuration_rejects_private_state_symlink_and_public_config(self):
        p=self.save();p.chmod(0o644)
        with self.assertRaises(PermissionError):configuration(p)
    def test_worker_environment_precedes_agent_import_and_requires_root(self):
        with patch.dict(os.environ,{},clear=True),patch('os.geteuid',return_value=0):
            configure_environment(self.cfg)
            self.assertEqual(os.environ['DL_AGENT_STATE_ROOT'],str(self.root));self.assertEqual(os.environ['DL_NODE_ID'],'pi02')
        with patch('os.geteuid',return_value=1000):
            with self.assertRaises(PermissionError):configure_environment(self.cfg)

class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();self.addCleanup(self.t.cleanup);self.root=Path(self.t.name)
    def archive(self,entries):
        raw=io.BytesIO()
        with tarfile.open(fileobj=raw,mode='w') as archive:
            for name,data,kind in entries:
                member=tarfile.TarInfo(name);member.type=kind;member.size=len(data);member.linkname='/etc/passwd'
                archive.addfile(member,io.BytesIO(data))
        raw.seek(0);return raw
    def valid(self):return [('lease.json',b'{}',tarfile.REGTYPE),('training.log',b'log',tarfile.REGTYPE),('results/metrics.json',b'{}',tarfile.REGTYPE)]
    def test_private_results_preserve_bytes(self):
        unpack(self.archive(self.valid()),self.root)
        self.assertEqual((self.root/'training.log').read_bytes(),b'log');self.assertEqual((self.root/'results/metrics.json').read_bytes(),b'{}')
    def test_links_traversal_special_unknown_and_duplicate_members_rejected(self):
        for name,kind in [('../escape',tarfile.REGTYPE),('/absolute',tarfile.REGTYPE),('results/link',tarfile.SYMTYPE),
                          ('results/hard',tarfile.LNKTYPE),('results/device',tarfile.CHRTYPE),('key.json',tarfile.REGTYPE)]:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):unpack(self.archive([(name,b'',kind)]),self.root)
        with self.assertRaises(ValueError):unpack(self.archive([('training.log',b'x',tarfile.REGTYPE)]*2),self.root)
    def test_missing_lease_or_log_rejected(self):
        with self.assertRaises(ValueError):unpack(self.archive([]),self.root)
    def test_aggregate_size_limit_enforced_before_file_creation(self):
        with patch('hybrid_storage.protected_results.MAX_BYTES',0):
            with self.assertRaises(ValueError):unpack(self.archive(self.valid()),self.root)

if __name__=='__main__':unittest.main(verbosity=2)

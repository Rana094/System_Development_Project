"""Trusted probe input/state safety and complete fake-NFS local rehearsal."""
import hashlib
from pathlib import Path
import sys
import json
import subprocess
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_cache_fetch as fixtures
from hybrid_storage.phase11_probe import Probe, fixture_files
from hybrid_storage.inventory_protocol import Credential
from hybrid_storage.inputs import object_key


class ProbeTests(unittest.TestCase):
    setUp=fixtures.FetchTests.setUp
    fetcher_handle=fixtures.FetchTests.fetcher_handle
    source_file=fixtures.FetchTests.source_file
    fake_spawn=fixtures.FetchTests.fake_spawn

    def files(self):
        data=[self.data,b'public two',b'public three']
        for value in data:self.source_file(value)
        return fixture_files({'files':[dict(path=f'part-{i}.bin',sha256=hashlib.sha256(value).hexdigest(),size_bytes=len(value))
                                       for i,value in enumerate(data)]})

    def test_manifest_refuses_traversal_duplicate_digests_zero_size_and_excessive_bytes(self):
        valid=[dict(path=f'p{i}',sha256=str(i)*64,size_bytes=1) for i in range(3)]
        for document in ({'files':[]},{'files':valid,'secret':'x'},
            {'files':[dict(v,path='../bad') if i==0 else v for i,v in enumerate(valid)]},
            {'files':[dict(v,sha256='a'*64) for v in valid]},
            {'files':[dict(v,size_bytes=0) for v in valid]},
            {'files':[dict(v,size_bytes=32*1024**2) for v in valid]}):
            with self.subTest(document=document),self.assertRaises(ValueError):fixture_files(document)

    def test_live_driver_defaults_to_plan_only_and_rejects_deferred_or_duplicate_nodes(self):
        command=[sys.executable,'-B',str(ROOT/'tools/validate_phase11.py'),'--nodes']
        before=set(self.base.iterdir())
        result=subprocess.run(command+['pi02','pi03'],capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr)
        plan=json.loads(result.stdout);self.assertEqual(plan['mode'],'plan_only')
        self.assertFalse(plan['production_agent_replacement']);self.assertEqual(set(self.base.iterdir()),before)
        for nodes in (['pi01'],['pi02','pi02']):
            result=subprocess.run(command+nodes,capture_output=True,text=True,timeout=5)
            self.assertNotEqual(result.returncode,0)

    def create(self,path):
        return Probe(path,worker='pi02',mount=self.nfs,source=fixtures.SOURCE,files=self.files(),
                     credential=Credential('pi02','a'*32,b'k'*32))

    def test_existing_state_never_reused_and_mount_rejection_creates_nothing(self):
        state=self.base/'retained';state.mkdir();(state/'keep').write_bytes(b'keep')
        with patch('hybrid_storage.phase11_probe.require_input_mount',return_value={'test':True}):
            with self.assertRaises(FileExistsError):self.create(state)
        self.assertEqual((state/'keep').read_bytes(),b'keep')
        absent=self.base/'never-created'
        with patch('hybrid_storage.phase11_probe.require_input_mount',side_effect=ValueError('no mount')):
            with self.assertRaises(ValueError):self.create(absent)
        self.assertFalse(absent.exists())

    def test_full_local_rehearsal_cold_warm_corrupt_concurrent_pins_crash_and_refetch(self):
        with patch('hybrid_storage.phase11_probe.require_input_mount',return_value={'fake_nfs':True}), \
                patch('hybrid_storage.cache_fetch.CacheFetcher._spawn',side_effect=self.fake_spawn):
            probe=self.create(self.base/'new-probe');self.addCleanup(probe.close)
            # This local test does not claim Linux privileged namespace coverage.
            with patch.object(probe,'isolation',return_value={'tested':False,'scope':'unit fixture'}):
                report=probe.bootstrap()
            self.assertTrue(all(report['checks'].values()));self.assertEqual(report['warm_staged_bytes'],0)
            self.assertEqual(report['cold_bytes'],sum(f.size_bytes for f in probe.files))
            before={f.path:(probe.cache.root/'objects'/object_key(f.sha256)).read_bytes() for f in probe.files}
            self.assertGreater(probe.evict_crash()['missing'],0)
            result=probe.restage();self.assertGreater(result['staged_bytes'],0)
            self.assertEqual(result['warm_staged_bytes'],0)
            self.assertEqual({f.path:(self.nfs/object_key(f.sha256)).read_bytes() for f in probe.files},before)


if __name__=='__main__':unittest.main(verbosity=2)

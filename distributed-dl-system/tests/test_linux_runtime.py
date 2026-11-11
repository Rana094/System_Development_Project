"""Disposable LOCAL boundary/metadata tests; no root, services or firewall edits."""
import copy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'agent'))
import test_storage_registry as fixtures
from hybrid_storage.linux_boundary import LinuxBoundary,LinuxConfig,atomic_json,protected_json,nft_policy,policy_digest,unit_name
from hybrid_storage.offline_prepare import OfflinePreparer
from hybrid_storage.input_manifests import encoded
from hybrid_storage.workspaces import AttemptIdentity,WorkspaceReceipt
from hybrid_storage.linux_guardian import Guardian


class LinuxTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.RegistryTests(methodName='runTest');self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.base=self.f.base
        self.worker=self.base/'worker';self.worker.mkdir(mode=0o700)
        self.state=self.base/'boundary';self.state.mkdir(mode=0o700)
        self.code=self.base/'code';self.code.mkdir()
        self.cfg=LinuxConfig(self.worker,self.code,self.state,599,599,('192.168.10.150','192.168.10.101'),'192.168.10.214','0'*64)
        self.identity=AttemptIdentity('job-test','job-test:0')
        self.lease=dict(node_id='pi02',generation=1,lease_id='a'*32,job_id='job-test',attempt_token='job-test:0',issued_at=1,expires_at=31)
        self.boundary=LinuxBoundary(self.cfg,command=Mock())

    def metadata(self):
        directory=self.worker/'worker-storage/jobs/job-test';directory.mkdir(parents=True,mode=0o700)
        atomic_json(directory/'request.json',dict(job_id='job-test'))
        env=self.worker/'worker-storage/environments'/('b'*64)
        atomic_json(directory/'prepared.json',dict(environment_key='b'*64,python_path=str(env/'bin/python')))
        return directory

    def launch(self,ddp=None):
        self.metadata()
        data=dict(job_id='job-test',attempt_token='job-test:0',lease=self.lease,
            resources=dict(cpu_cores=1,memory_gb=1,gpus=0),ddp=ddp)
        workspace=WorkspaceReceipt(self.identity.key,'c'*64,self.base/'workspace',True)
        self.boundary.preflight=Mock(return_value=True)
        return self.boundary.isolate(['/usr/bin/python3',str(self.code/'agent/custom_runner.py')],workspace,data)

    def test_construction_does_not_run_commands_or_create_files(self):
        self.boundary.command.assert_not_called();self.assertEqual(list(self.state.iterdir()),[])

    def test_invalid_identity_network_and_budgets_rejected(self):
        for kw in [dict(trainer_uid=0),dict(controller_ip='100.73.90.91'),dict(peers=('100.91.135.18',)),
                   dict(nft_table='bad; delete ruleset'),dict(service_port=29500),dict(max_output_bytes=-1),dict(policy_sha256='')]:
            with self.assertRaises(ValueError):replace(self.cfg,**kw)

    def test_policy_has_dedicated_uid_drop_and_empty_dynamic_peers(self):
        policy=nft_policy(self.cfg)
        self.assertIn('meta skuid 599 drop',policy)
        self.assertIn('ip daddr @active_peers tcp dport 20000-65535 accept',policy)
        self.assertNotIn('flush ruleset',policy);self.assertNotIn('192.168.10.150',policy)
        self.assertIn('ip saddr 192.168.10.214 tcp dport 5001 accept',policy)

    def policy(self):
        return dict(nftables=[dict(metainfo=dict(version='fixture')),dict(table=dict(family='inet',name=self.cfg.nft_table,handle=1)),
            dict(set=dict(family='inet',table=self.cfg.nft_table,name='active_peers',type='ipv4_addr',handle=2,elem=[])),
            dict(chain=dict(family='inet',table=self.cfg.nft_table,name='training_egress',handle=3))])

    def test_policy_hash_ignores_handles_and_allows_only_reviewed_peer_elements(self):
        p=self.policy();sha=policy_digest(p,self.cfg)
        p['nftables'][2]['set'].update(handle=99,elem=['192.168.10.101'])
        self.assertEqual(policy_digest(p,self.cfg),sha)
        p['nftables'][2]['set']['elem']=['192.168.10.214']
        with self.assertRaises(ValueError):policy_digest(p,self.cfg)

    def test_policy_hash_changes_on_rule_changes_and_rejects_other_tables(self):
        p=self.policy();sha=policy_digest(p,self.cfg)
        p['nftables'][3]['chain']['name']='changed'
        self.assertNotEqual(policy_digest(p,self.cfg),sha)
        p['nftables'][3]['chain']['table']='unrelated'
        with self.assertRaises(ValueError):policy_digest(p,self.cfg)

    def test_preflight_rejects_unprivileged_service(self):
        with patch('os.geteuid',return_value=1000):
            with self.assertRaises(PermissionError):self.boundary.preflight()

    def test_launch_records_intent_before_returning_bounded_systemd_command(self):
        argv=self.launch()
        record=protected_json(self.state/(self.identity.key+'.json'))
        self.assertEqual(record['phase'],'PREPARED');self.assertEqual(record['peers'],[])
        self.assertIn('--property=WatchdogSec=3s',argv);self.assertIn('--property=KillMode=control-group',argv)
        self.assertIn('--property=NoNewPrivileges=yes',argv);self.assertIn('--property=MemoryMax=1073741824',argv)
        self.assertIn('--property=BindsTo=dl-hybrid-node.service',argv)
        # These would isolate output mounts from the trusted agent. Bubblewrap
        # supplies the child /dev and hides /sys instead of service mount options.
        self.assertNotIn('--property=PrivateDevices=yes',argv)
        self.assertNotIn('--property=ProtectControlGroups=yes',argv)
        self.boundary.command.assert_not_called()

    def test_launch_refuses_unknown_ddp_group_without_creating_control_record(self):
        with self.assertRaises(PermissionError):self.launch(dict(master_addr='100.91.135.18',master_port=29500,world_size=2,rank=0))
        self.assertEqual(list(self.state.iterdir()),[])

    def test_known_two_rank_group_uses_only_reviewed_peers(self):
        self.launch(dict(master_addr='192.168.10.150',master_port=29500,world_size=2,rank=0))
        self.assertEqual(protected_json(self.state/(self.identity.key+'.json'))['peers'],list(self.cfg.peers))

    def test_world_size_and_rendezvous_control_port_cannot_be_changed_silently(self):
        self.metadata()
        receipt=WorkspaceReceipt(self.identity.key,'c'*64,self.base/'workspace',True)
        for size,port in ((3,29500),(2,5001)):
            data=dict(job_id='job-test',attempt_token='job-test:0',lease=self.lease,resources=dict(cpu_cores=1,memory_gb=1,gpus=0),
                ddp=dict(rank=0,world_size=size,master_addr='192.168.10.150',master_port=port))
            with self.assertRaises(PermissionError):self.boundary.isolate(['/usr/bin/python3',str(self.code/'agent/custom_runner.py')],receipt,data)

    def test_quiescence_requires_inactive_unit_and_exact_fence(self):
        self.launch()
        held=protected_json(self.state/(self.identity.key+'.json'))
        self.boundary.command.return_value=SimpleNamespace(stdout='LoadState=loaded\nActiveState=active\nControlGroup=\n')
        self.assertFalse(self.boundary.quiescent(self.identity,held))
        self.boundary.command.return_value=SimpleNamespace(stdout='LoadState=loaded\nActiveState=inactive\nControlGroup=\n')
        self.assertTrue(self.boundary.quiescent(self.identity,held))
        wrong=copy.deepcopy(held);wrong['lease']['generation']=2
        self.assertFalse(self.boundary.quiescent(self.identity,wrong))

    def test_missing_or_corrupt_record_is_not_quiescence_proof(self):
        self.assertFalse(self.boundary.quiescent(self.identity,dict(lease=self.lease)))

    def test_private_json_rejects_symlinks_public_modes_and_duplicates(self):
        p=self.state/'x.json';atomic_json(p,dict(x=1),new=True)
        with self.assertRaises(FileExistsError):atomic_json(p,dict(x=2),new=True)
        q=self.state/'link.json';q.symlink_to(p)
        with self.assertRaises(OSError):protected_json(q)
        p.chmod(0o644)
        with self.assertRaises(PermissionError):protected_json(p)

    def test_projected_copy_rejects_links_and_preserves_exact_bytes(self):
        guardian=Guardian.__new__(Guardian);guardian.authority=Mock()
        source=self.base/'input';source.write_bytes(b'bounded payload')
        target=self.base/'copied'
        self.assertEqual(guardian.copy_file(source,target,100),15)
        self.assertEqual(target.read_bytes(),source.read_bytes());self.assertEqual(target.stat().st_mode&0o777,0o444)
        link=self.base/'link';link.symlink_to(source)
        with self.assertRaises(OSError):guardian.copy_file(link,self.base/'reject',100)

    def test_projected_copy_refuses_size_over_budget_before_creating_output(self):
        guardian=Guardian.__new__(Guardian);guardian.authority=Mock()
        source=self.base/'input';source.write_bytes(b'payload')
        target=self.base/'reject'
        with self.assertRaises(PermissionError):guardian.copy_file(source,target,2)
        self.assertFalse(target.exists())


class OfflineTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.RegistryTests(methodName='runTest');self.addCleanup(self.f.doCleanups);self.f.setUp()
        self.worker=self.f.base/'offline-worker';self.worker.mkdir(mode=0o700)
        (self.worker/'worker-storage').mkdir(mode=0o700)
        site=self.f.base/'site';site.mkdir()
        self.capsule=dict(schema_version=1,site_root=str(site),runtime=dict(python='3.11.2',machine='aarch64',system='Linux'),
            packages=[['pip','25.3'],['torch','fixture']],profiles=['pytorch-cpu'],requirements_sha256=[hashlib.sha256(b'').hexdigest()])
        self.preparer=OfflinePreparer(self.worker,self.capsule)
        self.request=dict(job_id=self.f.job_id,profile='pytorch-cpu',manifest=json.loads(self.f.raw),manifest_sha256=hashlib.sha256(self.f.raw).hexdigest())
        self.request['manifest']['execution_spec']['environment']['python']='3.11'
        self.request['manifest_sha256']=hashlib.sha256(encoded(self.request['manifest'])).hexdigest()
        def probe(path):return dict(packages=self.capsule['packages'],prefix=str(Path(path).parent.parent))
        self.probe=patch('environment_worker.probe',side_effect=probe).start();self.addCleanup(patch.stopall)
        self.pip=patch('environment_worker.pip_check',return_value='No broken requirements found').start()

    def test_metadata_prepare_reuses_approved_capsule_without_copying_inputs(self):
        result=self.preparer.prepare(self.request);record=result['record']
        self.assertEqual(record['manifest_sha256'],self.request['manifest_sha256'])
        self.assertFalse((Path(record['workspace_path'])/'source').exists())
        self.assertFalse((Path(record['workspace_path'])/'data').exists())
        self.assertEqual(self.preparer.prepare(self.request),result)
        self.probe.assert_called_once();self.pip.assert_called_once()

    def test_unapproved_requirements_rejected_before_creating_job_or_environment(self):
        self.capsule['requirements_sha256']=['f'*64]
        with self.assertRaises(PermissionError):OfflinePreparer(self.worker,self.capsule).prepare(self.request)
        self.assertEqual(list((self.worker/'worker-storage').iterdir()),[])

    def test_wrong_manifest_hash_and_profile_rejected_without_execution(self):
        for value in (dict(self.request,manifest_sha256='f'*64),dict(self.request,profile='worker-base')):
            with self.assertRaises(PermissionError):self.preparer.prepare(value)
        self.probe.assert_not_called();self.pip.assert_not_called()

    def test_different_existing_job_metadata_is_not_overwritten(self):
        self.preparer.prepare(self.request)
        changed=copy.deepcopy(self.request);changed['manifest']['owner']='mallory'
        changed['manifest_sha256']=hashlib.sha256(encoded(changed['manifest'])).hexdigest()
        with self.assertRaises(PermissionError):self.preparer.prepare(changed)
        actual=protected_json(self.worker/'worker-storage/jobs'/self.f.job_id/'request.json')
        self.assertEqual(actual,self.request)

    def test_failed_environment_probe_is_not_deleted_or_adopted_on_retry(self):
        self.probe.side_effect=ValueError('interrupted capsule verification')
        with self.assertRaises(ValueError):self.preparer.prepare(self.request)
        self.probe.side_effect=None
        with self.assertRaises(PermissionError):self.preparer.prepare(self.request)
        self.assertEqual(len(list((self.worker/'worker-storage/environments').iterdir())),1)


if __name__=='__main__':unittest.main(verbosity=2)

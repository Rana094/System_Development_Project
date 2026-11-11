"""Disposable validation and rollback contracts; no root/service/network writes."""
from pathlib import Path
import json
import os
import socket
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from hybrid_storage import pilot_provision as p

class ProvisionTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();self.addCleanup(self.t.cleanup);self.root=Path(self.t.name)
    def test_source_catalog_rejects_extra_missing_links_and_traversal(self):
        source=self.root/'payload';source.mkdir();(source/'a.py').write_bytes(b'print(1)')
        plan=dict(files={'a.py':p.digest(b'print(1)')});p.verify_payload(plan,source)
        (source/'extra').write_bytes(b'')
        with self.assertRaises(ValueError):p.verify_payload(plan,source)
        (source/'extra').unlink();(source/'a.py').unlink();(source/'a.py').symlink_to('/etc/passwd')
        with self.assertRaises(ValueError):p.verify_payload(plan,source)
        with self.assertRaises(ValueError):p.verify_payload(dict(files={'../bad':'0'*64}),source)
    def test_tree_identity_changes_on_content_executable_or_link_changes(self):
        d=self.root/'site';d.mkdir();f=d/'a.py';f.write_bytes(b'a');first=p.tree_inventory(d)
        (d/'__pycache__').mkdir();(d/'__pycache__/ignore.pyc').write_bytes(b'ignored')
        self.assertEqual(first,p.tree_inventory(d))
        f.chmod(0o755);self.assertNotEqual(first,p.tree_inventory(d));f.chmod(0o644)
        f.write_bytes(b'b');self.assertNotEqual(first,p.tree_inventory(d))
        (d/'escape').symlink_to('/etc/passwd')
        with self.assertRaises(ValueError):p.tree_inventory(d)
    def test_existing_data_is_never_replaced_or_repaired(self):
        target=self.root/'keep';p.write(target,b'original');p.write(target,b'original')
        with self.assertRaises(PermissionError):p.write(target,b'new')
        self.assertEqual(target.read_bytes(),b'original')
    def test_owned_sudo_policy_rotates_once_with_backup_and_refuses_foreign_policy(self):
        state=self.root/'state';state.mkdir();target=self.root/'sudo-policy'
        current=dict(release='pilot-20261111-123456abcdef',operator_name='rana')
        prior=p.sudo_policy(current,'pilot-20261110-abcdef123456','1'*64)
        target.write_bytes(prior);target.chmod(0o440)
        wanted=p.sudo_policy(current)
        self.assertEqual(p.install_sudo_policy(current,wanted,state,target)['status'],'OWNED_RELEASE_FILE_ROTATED')
        self.assertEqual(target.read_bytes(),wanted);self.assertEqual((state/'prior-owned-files'/target.name).read_bytes(),prior)
        self.assertEqual(p.install_sudo_policy(current,wanted,state,target)['status'],'CURRENT_RELEASE_FILE_ALREADY_INSTALLED')
        target.chmod(0o600);target.write_bytes(b'rana ALL=(ALL) NOPASSWD: ALL\n');target.chmod(0o440)
        with self.assertRaises(PermissionError):p.install_sudo_policy(current,wanted,self.root/'other-state',target)
    def test_owned_units_and_bridge_rotate_only_exact_prior_release(self):
        state=self.root/'state';state.mkdir();release='pilot-20261111-123456abcdef'
        plan=dict(release=release,operator_uid=1000,operator_gid=1000,operator_home='/home/operator',
            nodes=[dict(id='pi02',ip='192.168.10.150',worker_root='/var/lib/dl-hybrid-pilot/'+release+'/worker')])
        old='pilot-20261110-abcdef123456';oldcode=Path('/opt/dl-hybrid-pilot')/old/'app';oldstate=Path('/var/lib/dl-hybrid-pilot')/old
        target=self.root/'node.service';target.write_bytes(p.service_unit(dict(plan,code_root=str(oldcode)),'worker',oldcode.parent/'service/bin/python',oldstate/'control/config-worker.json'));target.chmod(0o644)
        code=Path('/opt/dl-hybrid-pilot')/release/'app';wanted=p.service_unit(dict(plan,code_root=str(code)),'worker',code.parent/'service/bin/python',Path('/var/lib/dl-hybrid-pilot')/release/'control/config-worker.json')
        result=p.install_owned_release_file(plan,wanted,state,target,0o644,lambda raw:p._owned_prior_unit(plan,'worker',raw))
        self.assertEqual(result['status'],'OWNED_RELEASE_FILE_ROTATED');self.assertEqual(target.read_bytes(),wanted)
        bridge=self.root/'result-bridge.json';prior_bridge=p.result_bridge_document(plan,old)
        self.assertEqual(prior_bridge['nodes']['pi02']['worker_root'],'/var/lib/dl-hybrid-pilot/'+old+'/worker')
        bridge.write_bytes(p.encoded(prior_bridge));bridge.chmod(0o600)
        current=p.encoded(p.result_bridge_document(plan));p.install_owned_release_file(plan,current,state,bridge,0o600,lambda raw:p._owned_prior_bridge(plan,raw))
        self.assertEqual(bridge.read_bytes(),current)
        self.assertIn(release,json.loads(current)['nodes']['pi02']['worker_root'])
        foreign=self.root/'foreign.service';foreign.write_bytes(b'[Service]\nExecStart=/bin/true\n');foreign.chmod(0o644)
        with self.assertRaises(PermissionError):p.install_owned_release_file(plan,wanted,state,foreign,0o644,lambda raw:p._owned_prior_unit(plan,'worker',raw))
    def test_plan_requires_pinned_host_role_private_roots_and_exact_scope(self):
        plan=dict(schema_version=1,role='worker',host='pi02',hostname=socket.gethostname(),release='pilot-20261111-123456abcdef',
            system_python=p.platform.python_version(),selected_workers=['pi02','pi03'],deferred_workers=['pi01'],
            code_root='/opt/dl-hybrid-pilot/pilot-20261111-123456abcdef/app',state_base='/var/lib/dl-hybrid-pilot/pilot-20261111-123456abcdef')
        path=self.root/'plan.json';path.write_bytes(p.encoded(plan));self.assertEqual(p.load_plan(path,p.digest(path.read_bytes())),plan)
        for key,value in [('hostname','wrong'),('host','pi01'),('code_root','/tmp/code'),('state_base','/srv/nfs/db'),('deferred_workers',[])]:
            bad=dict(plan);bad[key]=value;path.write_bytes(p.encoded(bad))
            with self.assertRaises(ValueError):p.load_plan(path,p.digest(path.read_bytes()))
        with self.assertRaises(ValueError):p.load_plan(path,'0'*64)
    def test_worker_unit_boot_is_nonblocking_and_uses_guard_before_agent(self):
        unit=p.service_unit(dict(code_root='/opt/release/app'),'worker',Path('/opt/service/bin/python'),Path('/var/lib/profile.json')).decode()
        self.assertIn('systemctl --no-block start srv-mlnfs-inputs.mount',unit)
        self.assertLess(unit.index('hybrid_pilot_guard.py'),unit.index('ExecStart='))
        self.assertNotIn('Requires=srv-mlnfs',unit);self.assertIn('User=root',unit)
        portal=p.service_unit(dict(code_root='/opt/release/app'),'portal',Path('/opt/service/bin/python'),Path('/var/lib/profile.json')).decode()
        self.assertIn('User=ml-publisher',portal);self.assertNotIn('ExecStartPre=',portal)
    def test_drain_race_restores_legacy_services_and_does_not_touch_workers(self):
        plan=dict(role='controller',host='controller',state_base=str(self.root))
        commands=[]
        def control(plan,*argv,**kw):
            commands.append(argv)
            return Mock(stdout='ActiveState=active\nUnitFileState=enabled\n')
        with patch.object(p,'idle_controller',side_effect=[None,PermissionError('Concurrent dispatch')]),patch.object(p,'systemctl',side_effect=control):
            with self.assertRaises(PermissionError):p.drain(plan)
        self.assertIn(('start','dl-student-portal.service'),commands)
        self.assertIn(('start','dl-dispatcher.service'),commands)
        self.assertFalse((self.root/'DRAINED.json').exists())
        self.assertFalse(any('dl-node-agent.service' in a for a in commands))
    def test_activation_requires_prepared_release_and_controller_drain(self):
        plan=dict(role='controller',state_base=str(self.root),code_root='/opt/release/app')
        with self.assertRaises(PermissionError):p.activate(plan)
        (self.root/'PREPARED.json').write_bytes(b'{}')
        with self.assertRaises(PermissionError):p.activate(plan)
    def test_rollback_restores_only_original_active_and_enabled_states(self):
        plan=dict(role='controller',host='controller',state_base=str(self.root))
        p.write(self.root/'LEGACY-SERVICES.json',{'dl-dispatcher.service':'ActiveState=active\nUnitFileState=enabled\n',
            'dl-student-portal.service':'ActiveState=inactive\nUnitFileState=disabled\n'})
        control=Mock()
        with patch.object(p,'systemctl',control):p.rollback(plan)
        control.assert_any_call(plan,'enable','dl-dispatcher.service',legacy=True)
        control.assert_any_call(plan,'start','dl-dispatcher.service',legacy=True)
        self.assertFalse(any('dl-student-portal.service' in call.args and 'start' in call.args for call in control.call_args_list))
        self.assertTrue((self.root/'LEGACY-SERVICES.json').exists())

if __name__=='__main__':unittest.main(verbosity=2)

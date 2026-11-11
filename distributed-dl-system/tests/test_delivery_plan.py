"""Pi-less delivery review safety: no production DB/service/network operations."""
import contextlib
from concurrent.futures import ThreadPoolExecutor
import gzip
import importlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from hybrid_storage import delivery_plan as delivery
from hybrid_storage import deployment_review as source


class DeliveryPlanTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name);self.root=self.base/'source';self.root.mkdir()
        names=set().union(*(source.catalog(role,root=ROOT) for role in source.ROLES))
        names.update(('config/nodes.json','config/storage.json',delivery.AUDIT))
        names.update('config/'+name for name in delivery.PUBLIC_CONFIGS)
        names.add('tools/hybrid_deploy.py')
        names.update('hybrid_storage/'+module+'.py' for module in delivery.MIGRATIONS)
        for name in names:
            target=self.root/name;target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes((ROOT/name).read_bytes())
        audit=json.loads((self.root/delivery.AUDIT).read_text())
        audit['repository']['app_root']=str(self.root)
        for service in audit['hosts']['controller']['observed']['services'].values():
            if 'WorkingDirectory' in service['properties']:
                service['properties']['WorkingDirectory']=str(self.root)
        (self.root/delivery.AUDIT).write_text(json.dumps(audit))
        self.output=self.base/'review'

    def make(self):return delivery.prepare(self.output,root=self.root)

    def tree(self):
        return {str(p.relative_to(self.output)):(p.read_bytes(),p.stat().st_mode,p.stat().st_mtime_ns)
                for p in self.output.rglob('*') if p.is_file()}

    def test_offline_all_three_artifacts_do_not_read_runtime_or_execute_commands(self):
        # A private runtime file is deliberately a FIFO: any attempted DB read hangs.
        fifo=self.root/'controller/jobs.db';fifo.parent.mkdir(exist_ok=True);os.mkfifo(fifo)
        import sqlite3
        with patch('socket.socket',side_effect=AssertionError('network forbidden')), \
             patch('subprocess.run',side_effect=AssertionError('commands forbidden')), \
             patch.object(sqlite3,'connect',side_effect=AssertionError('DB forbidden')):
            result=self.make()
            self.assertEqual(delivery.verify(self.output,root=self.root),result)
        self.assertTrue(result['verified']);self.assertFalse(result['apply_ready'])
        self.assertFalse(result['live_readiness_verified'])
        self.assertEqual(result['selected_workers'],['pi01','pi02','pi03'])
        plan=json.loads((self.output/'plan.json').read_text())
        self.assertEqual(plan['hosts']['pi03']['audited_uid'],1002)
        self.assertEqual(plan['hosts']['pi01']['ssh']['hostname'],'100.101.90.46')
        self.assertFalse(plan['hosts']['pi01']['ssh']['host_key_alias_is_current_lan'])
        self.assertEqual(plan['configuration']['current']['storage_backend'],'legacy')
        self.assertTrue(all(not s['executable'] for s in plan['steps']))
        self.assertTrue(all(not c['execute'] and c['approval_required'] for c in plan['restart_plan']['commands']))
        for role in source.ROLES:
            self.assertIn('hybrid_storage/worker_cache.py',plan['source_catalogs'][role])
            self.assertIn('hybrid_storage/delivery_plan.py',plan['source_catalogs'][role])
            self.assertNotIn('controller/jobs.db',plan['source_catalogs'][role])
        self.assertEqual(plan['migrations']['db_path'],str(self.root/'controller/jobs.db'))
        self.assertFalse(plan['migrations']['execute'])
        self.assertIsNone(plan['configuration']['new_runtime_file'])

    def test_complete_retry_is_byte_for_byte_idempotent_and_does_not_refresh_backup_id(self):
        first=self.make();before=self.tree()
        self.assertEqual(self.make(),first);self.assertEqual(self.tree(),before)
        second=delivery.prepare(self.base/'second',root=self.root)
        self.assertEqual(first['release_id'],second['release_id'])
        self.assertNotEqual(first['snapshot_id'],second['snapshot_id'])
        self.assertEqual(self.output.stat().st_mode&0o777,0o700)
        for p in self.output.rglob('*'):
            if p.is_file():self.assertEqual(p.stat().st_mode&0o777,0o600)

    def test_concurrent_same_destination_never_replaces_an_incomplete_or_complete_review(self):
        def attempt(_):
            try:return self.make()
            except (OSError,ValueError):return None  # safe refusal while first writer owns incomplete output
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(attempt,range(2)))
        successful=[r for r in results if r is not None]
        self.assertTrue(successful)
        final=delivery.verify(self.output,root=self.root)
        self.assertTrue(all(r==final for r in successful))
        before=self.tree();self.assertEqual(self.make(),final);self.assertEqual(self.tree(),before)

    def test_config_audit_and_source_drift_refuse_existing_output_without_overwrite(self):
        self.make();before=self.tree()
        for name in ('config/storage.json',delivery.AUDIT,'agent/staging.py'):
            with self.subTest(name=name):
                target=self.root/name;old=target.read_bytes();target.write_bytes(old+b'\n')
                try:
                    with self.assertRaises(ValueError):self.make()
                    self.assertEqual(self.tree(),before)
                finally:target.write_bytes(old)

    def test_missing_pi01_evidence_or_mismatched_alias_is_not_fabricated(self):
        target=self.root/delivery.AUDIT;old=target.read_bytes()
        for change in ('missing','spoof'):
            with self.subTest(change=change):
                audit=json.loads(old)
                if change=='missing':del audit['hosts']['pi01']
                else:audit['ssh_aliases']['pi01']['resolved']['hostname']=['192.168.10.125']
                target.write_text(json.dumps(audit))
                with self.assertRaisesRegex(ValueError,'evidence missing|identity mismatch'):self.make()
                self.assertFalse(self.output.exists())
        target.write_bytes(old)

    def test_nonlegacy_active_config_cannot_be_smuggled_into_preparation(self):
        (self.root/'config/storage.json').write_text(json.dumps(dict(storage_backend='nfs',
            worker_cache_enabled=True,storage_aware_staging=True,scheduler_policy='locality')))
        with self.assertRaises(ValueError):self.make()
        self.assertFalse(self.output.exists())

    def test_proposed_migration_order_twice_on_restored_actual_legacy_fixture(self):
        # Existing fixture copies controller code and redirects every DB/runtime root.
        import sqlite3
        import test_storage_registry as fixtures
        fixture=fixtures.RegistryTests(methodName='runTest')
        self.addCleanup(fixture.doCleanups);fixture.setUp()
        fixture.db.execute('PRAGMA journal_mode=WAL')
        fixture.db.execute('PRAGMA user_version=88');fixture.db.commit()
        tables=[r[0] for r in fixture.db.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        legacy={t:list(fixture.db.execute('SELECT * FROM '+t)) for t in tables}
        restored=self.base/'restored.sqlite'
        with contextlib.closing(sqlite3.connect(restored)) as copy:
            fixture.db.backup(copy)
            copy.execute('PRAGMA foreign_keys=ON');copy.execute('PRAGMA synchronous=FULL')
            modules=[importlib.import_module('hybrid_storage.'+m) for m in delivery.MIGRATIONS]
            for module in modules:module.migrate(copy)
            first=list(copy.iterdump())
            for module in modules:module.migrate(copy)
            self.assertEqual(list(copy.iterdump()),first)
            self.assertEqual(copy.execute('PRAGMA quick_check').fetchone()[0],'ok')
            self.assertEqual(list(copy.execute('PRAGMA foreign_key_check')),[])
            self.assertEqual(copy.execute('PRAGMA user_version').fetchone()[0],88)
            for table,rows in legacy.items():self.assertEqual(list(copy.execute('SELECT * FROM '+table)),rows)
        for table,rows in legacy.items():self.assertEqual(list(fixture.db.execute('SELECT * FROM '+table)),rows)

    def test_partial_output_retry_refuses_and_retains_written_evidence(self):
        original=delivery._write
        def interrupt(parent,name,data):
            if name=='delivery.json':raise OSError('simulated interrupted publication')
            return original(parent,name,data)
        with patch.object(delivery,'_write',side_effect=interrupt), self.assertRaises(OSError):self.make()
        self.assertFalse((self.output/'delivery.json').exists());before=self.tree()
        with self.assertRaises(FileNotFoundError):self.make()
        self.assertEqual(self.tree(),before)

    def test_source_change_during_build_never_completes_plan(self):
        original=source.build
        def changing(*args,**kwargs):
            result=original(*args,**kwargs)
            with (self.root/'agent/staging.py').open('ab') as stream:stream.write(b'\n# intervening edit\n')
            return result
        with patch.object(source,'build',side_effect=changing),self.assertRaisesRegex(ValueError,'Inputs changed'):self.make()
        self.assertFalse((self.output/'delivery.json').exists())

    def test_untrusted_paths_links_and_existing_files_are_not_overwritten(self):
        with self.assertRaises(ValueError):delivery.prepare(Path('relative'),root=self.root)
        real=self.base/'real';real.mkdir();alias=self.base/'alias';alias.symlink_to(real,target_is_directory=True)
        with self.assertRaises(OSError):delivery.prepare(alias/'new',root=self.root)
        self.assertFalse((real/'new').exists())
        keep=self.base/'file';keep.write_text('preserve')
        with self.assertRaises(OSError):delivery.prepare(keep,root=self.root)
        self.assertEqual(keep.read_text(),'preserve')
        self.make();saved=self.output/'saved.json'
        (self.output/'delivery.json').rename(saved);(self.output/'delivery.json').symlink_to(saved)
        with self.assertRaises(OSError):delivery.verify(self.output,root=self.root)

    def test_plan_edit_duplicate_json_or_extra_file_invalidates_review(self):
        self.make();target=self.output/'plan.json';old=target.read_bytes()
        for raw in (old.replace(b'"apply_ready": false',b'"apply_ready": true',1),b'{"a":1,"a":2}'):
            target.write_bytes(raw)
            with self.assertRaises(ValueError):delivery.verify(self.output,root=self.root)
        target.write_bytes(old);(self.output/'unexpected').write_text('extra')
        with self.assertRaisesRegex(ValueError,'Unexpected files'):delivery.verify(self.output,root=self.root)

    def reseal_archive(self):
        path=self.output/'source/worker-source.tar.gz'
        review_path=self.output/'source/review.json';review=json.loads(review_path.read_text())
        review['archives']['worker'].update(delivery._digest(path.read_bytes()))
        review_path.write_bytes(delivery._json(review))
        marker_path=self.output/'delivery.json';marker=json.loads(marker_path.read_text())
        marker['files']['source/worker-source.tar.gz']=delivery._digest(path.read_bytes())
        marker['files']['source/review.json']=delivery._digest(review_path.read_bytes())
        marker_path.write_bytes(delivery._json(marker))

    def test_archive_digest_tamper_is_rejected(self):
        self.make();path=self.output/'source/worker-source.tar.gz'
        with path.open('ab') as stream:stream.write(b'corrupt')
        with self.assertRaisesRegex(ValueError,'digest/size'):delivery.verify(self.output,root=self.root)

    def test_resealed_compressed_extended_header_cannot_exceed_expansion_budget(self):
        self.make();path=self.output/'source/worker-source.tar.gz'
        header=tarfile.TarInfo('oversized-pax');header.type=tarfile.XHDTYPE;header.size=4*1024*1024
        path.write_bytes(gzip.compress(header.tobuf()+b'\0'*(128*1024)))
        self.reseal_archive()
        with patch.object(delivery,'EXPANDED_LIMIT',4096), \
                self.assertRaisesRegex(ValueError,'Expanded archive exceeds'):
            delivery.verify(self.output,root=self.root)

    def test_even_resealed_traversal_symlink_and_missing_member_archives_are_rejected(self):
        self.make();path=self.output/'source/worker-source.tar.gz';original=path.read_bytes()
        for attack in ('traversal','symlink','missing','duplicate'):
            with self.subTest(attack=attack):
                with tarfile.open(fileobj=io.BytesIO(original),mode='r:gz') as src:
                    entries=[(m,src.extractfile(m).read()) for m in src]
                with tarfile.open(path,mode='w:gz') as dest:
                    for i,(member,data) in enumerate(entries):
                        if i==0:
                            if attack=='missing':continue
                            if attack=='traversal':member.name='../escape'
                            if attack=='symlink':member.type=tarfile.SYMTYPE;member.linkname='/etc/passwd';member.size=0
                        dest.addfile(member,io.BytesIO(data) if member.isfile() else None)
                        if i==0 and attack=='duplicate':dest.addfile(member,io.BytesIO(data))
                self.reseal_archive()
                with self.assertRaisesRegex(ValueError,'archive member|member missing'):delivery.verify(self.output,root=self.root)
                self.assertFalse((self.base/'escape').exists())
                path.write_bytes(original)

    def test_apply_has_no_executor_even_for_tampered_or_nonexistent_output(self):
        self.make();target=self.output/'plan.json';plan=json.loads(target.read_text());plan['apply_ready']=True
        target.write_text(json.dumps(plan))
        for output in (self.output,self.base/'not-there'):
            with patch.object(delivery,'prepare',side_effect=AssertionError('must not prepare')), \
                 patch.object(delivery,'verify',side_effect=AssertionError('must not read package')), \
                 contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(delivery.main(['apply','--output',str(output)]),3)
                self.assertEqual(json.loads(stdout.getvalue())['status'],'REFUSED')

    def test_real_CLI_prepare_verify_and_apply_without_database_or_network_import_effects(self):
        destination=self.base/'cli'
        env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1')
        code='''import sys
from pathlib import Path
from hybrid_storage.delivery_plan import main
assert 'sqlite3' not in sys.modules and 'job_store' not in sys.modules
assert main(['plan','--output',sys.argv[1]],root=Path(sys.argv[2]))==0
assert main(['verify','--output',sys.argv[1]],root=Path(sys.argv[2]))==0
assert main(['apply','--output',sys.argv[1]])==3
assert 'sqlite3' not in sys.modules and 'job_store' not in sys.modules
'''
        result=subprocess.run([sys.executable,'-B','-c',code,str(destination),str(self.root)],cwd=ROOT,env=env,
                              capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        # Actual public wrapper must return a nonzero refusal, not simulate deployment.
        refused=subprocess.run([sys.executable,'-B',str(ROOT/'tools/hybrid_deploy.py'),'apply',
            '--output',str(destination)],cwd=ROOT,env=env,capture_output=True,text=True,timeout=10)
        self.assertEqual(refused.returncode,3);self.assertEqual(json.loads(refused.stdout)['status'],'REFUSED')


if __name__=='__main__':unittest.main(verbosity=2)

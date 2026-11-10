"""13.2 protocol and real cache/registry components; no production/Pi mutation."""
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools/benchmarks'))
from evaluation import matrix,runner,datasets,matrix_context


class PlanTests(unittest.TestCase):
    def settings(self,**changes):
        return matrix.MatrixSettings(runner.Settings('matrix-test',rows=(12,),workloads=('regression',)),source_mode='local-rehearsal',**changes)

    def test_all_four_configs_repeated_pairing_and_order_reproduce_exactly(self):
        s=self.settings();order=matrix.schedule(s)
        self.assertEqual(len(order),24);self.assertEqual(order,matrix.schedule(s))
        self.assertNotEqual(order,matrix.schedule(replace(s,order_seed=42)))
        for block in range(1,4):
            for config in matrix.CONFIGS:
                pair=[e['repeat_phase'] for e in order if e['block']==block and e['config']==config]
                self.assertEqual(pair,['first','repeat'])
        self.assertEqual(matrix.CONFIGS['D'],dict(input_mode='stage',persistent_reuse=True,scheduler_policy='locality'))

    def test_bad_bounds_source_modes_and_external_rehearsal_paths_refused(self):
        s=self.settings()
        for changed in (dict(repeats=1),dict(repeats=True),dict(source_mode='fake-nfs'),dict(input_root=Path('/tmp')),dict(nfs_source='bad'),dict(order_seed=-1)):
            with self.subTest(changed=changed),self.assertRaises(ValueError):replace(s,**changed).validate()

    def test_nfs_run_requires_real_mount_before_creating_any_report(self):
        s=replace(self.settings(),source_mode='nfs',input_root=Path('/srv/mlnfs/inputs'),nfs_source='192.0.2.10:/srv/ml-shared/objects',trusted_direct_inputs=True)
        with tempfile.TemporaryDirectory() as d,patch.object(matrix,'require_input_mount',side_effect=ValueError('not mounted')):
            p=matrix.plan(s);self.assertEqual(p['nfs_input']['readiness'],'not_ready')
            with self.assertRaises(ValueError):matrix.run(s,Path(d))
            self.assertEqual(list(Path(d).iterdir()),[])

    def test_no_general_student_direct_nfs_opt_in(self):
        s=replace(self.settings(),source_mode='nfs',input_root=Path('/srv/mlnfs/inputs'),nfs_source='192.0.2.10:/srv/ml-shared/objects')
        with tempfile.TemporaryDirectory() as d,patch.object(matrix,'require_input_mount',return_value={}):
            with self.assertRaises(PermissionError):matrix.run(s,Path(d))
            self.assertEqual(list(Path(d).iterdir()),[])

    def test_summary_keeps_failures_raw_distributions_and_honest_small_n(self):
        common=dict(workload='regression',rows=12,world_size=1,config='C',repeat_phase='first')
        rows=[dict(common,status='COMPLETED',total_ns=n,staging_ns=n//2) for n in (10,20,30)]
        rows.append(dict(common,status='FAILED',total_ns=999,staging_ns=999))
        result=matrix.summarize(rows)[0]
        self.assertEqual((result['attempted'],result['completed'],result['failed']),(4,3,1))
        self.assertFalse(result['all_trials_completed']);self.assertEqual(result['total_ns']['samples'],[10,20,30])
        self.assertEqual(result['total_ns']['mean'],20);self.assertEqual(result['total_ns']['stdev'],10)
        self.assertEqual(result,matrix.summarize(rows)[0]);self.assertIsNotNone(result['total_ns']['mean_bootstrap95'])
        self.assertIsNone(matrix.distribution([1,2],1)['mean_bootstrap95']);self.assertIsNone(matrix.distribution([],1)['mean'])

    def test_mixed_scope_inputs_or_invalid_durations_cannot_form_comparison(self):
        row=dict(workload='regression',rows=12,world_size=1,config='C',repeat_phase='first',status='COMPLETED',total_ns=10,staging_ns=2)
        for field in ('scope','workload_sha256','dataset_sha256','dataset_version','dataset_id'):
            with self.assertRaises(ValueError):matrix.summarize([dict(row,**{field:'one'}),dict(row,**{field:'two'})])
        for value in (True,-1,float('nan'),1.5,2**63):
            with self.assertRaises(ValueError):matrix.distribution([value],1)


class ComponentTests(unittest.TestCase):
    def context(self,world=1):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name)
        recipe=datasets.generate(root,'ddp' if world>1 else 'regression',12,123,max_bytes=10000)
        c=matrix_context.Context(root/'context',None);self.addCleanup(c.close)
        c.register(ROOT/'examples'/('ddp_project' if world>1 else 'checkpoint_project'),root/recipe['path'],world)
        return c

    def test_cold_then_warm_real_fetch_and_private_workspace_never_writes_cache(self):
        c=self.context();selected,choice=c.choose('resource',1)
        receipts,plans,cold=c.stage(selected,1);self.assertGreater(cold['transferred_bytes'],0);self.assertEqual(cold['cache_hit_bytes'],0)
        for manager,plan in plans:manager.finish(plan,'COMPLETED',quiescent=lambda _:True)
        receipts,plans,warm=c.stage(selected,2)
        self.assertEqual(warm['transferred_bytes'],0);self.assertEqual(warm['cache_hit_bytes'],cold['transferred_bytes'])
        cached=c.worker(selected[0])[0]
        data=c.data.entries[0];file=Path(receipts[0]['dataset'])
        self.assertNotEqual(file.stat().st_ino,(cached.root/'objects'/matrix_context.object_key(data.sha256)).stat().st_ino)
        for manager,plan in plans:manager.finish(plan,'COMPLETED',quiescent=lambda _:True)
        self.assertTrue(all(not r.pinned_references for r in cached.inventory()))

    def test_locality_uses_real_signed_verified_cache_on_resource_tie(self):
        c=self.context();_,plans,_=c.stage(['slot-02'],1)
        for manager,plan in plans:manager.finish(plan,'COMPLETED',quiescent=lambda _:True)
        resource,_=c.choose('resource',1);locality,choice=c.choose('locality',1)
        self.assertEqual(resource,['slot-00']);self.assertEqual(locality,['slot-02'])
        missing={r['worker_id']:r['missing_bytes'] for r in choice['coverage']}
        self.assertEqual(missing['slot-02'],0);self.assertGreater(missing['slot-00'],0)
        self.assertEqual(choice['locality']['policy'],'locality')

    def test_group_actual_full_input_coverage_and_stale_inventory_fallback(self):
        c=self.context(world=2);_,plans,_=c.stage(['slot-01','slot-02'],1)
        for manager,plan in plans:manager.finish(plan,'COMPLETED',quiescent=lambda _:True)
        selected,choice=c.choose('locality',2);self.assertEqual(selected,['slot-01','slot-02'])
        self.assertEqual(choice['locality']['per_rank_missing_bytes'],(0,0))
        c.inventory.offline('slot-02');selected,choice=c.choose('locality',2,refresh=False)
        self.assertEqual(selected,['slot-00','slot-01']);self.assertEqual(choice['locality']['policy'],'resource')

    def test_public_input_preparation_never_publishes_or_executes(self):
        s=matrix.MatrixSettings(runner.Settings('prepare',rows=(12,),workloads=('regression',)),source_mode='local-rehearsal')
        with tempfile.TemporaryDirectory() as d,patch.object(matrix,'train',side_effect=AssertionError('No trainer')):
            folder,record=matrix.prepare_inputs(s,Path(d))
            self.assertEqual(record['status'],'PREPARED_NOT_PUBLISHED');self.assertEqual(record['trial_count'],0)
            for item in record['artifacts']:
                p=folder/'objects'/item['object_key'];self.assertEqual(datasets.digest(p),item['sha256'])
                self.assertEqual(p.stat().st_size,item['bytes'])

    def test_matrix_real_staging_no_reuse_vs_reuse_and_failure_retention(self):
        s=matrix.MatrixSettings(runner.Settings('matrix',rows=(12,),workloads=('regression',)),repeats=2,source_mode='local-rehearsal')
        def failed(*args):return dict(status='FAILED',ranks=[],exit_code=7,trainer_quiescent=False)
        with tempfile.TemporaryDirectory() as d,patch.object(matrix,'train',side_effect=failed):
            folder,record=matrix.run(s,Path(d));self.assertEqual(record['status'],'FAILED')
            rows=[json.loads(x) for x in (folder/'trials.jsonl').read_text().splitlines()]
            self.assertEqual(len(rows),16);self.assertTrue(all(r['status']=='FAILED' for r in rows))
            for row in rows:
                if row['config']=='B':self.assertGreater(row['transferred_bytes'],0)
                if row['config'] in ('C','D') and row['repeat_phase']=='repeat':self.assertEqual(row['transferred_bytes'],0)
                if row['config']=='A':self.assertIsNone(row['transferred_bytes'])
            self.assertEqual(len({r['workload_sha256'] for r in rows}),1)
            self.assertEqual(len({r['dataset_sha256'] for r in rows}),1)
            self.assertEqual(len({r['dataset_id'] for r in rows}),1)
            self.assertEqual(json.loads((folder/'summary.json').read_text())['scope'],'local_component_rehearsal')


if __name__=='__main__':unittest.main(verbosity=2)

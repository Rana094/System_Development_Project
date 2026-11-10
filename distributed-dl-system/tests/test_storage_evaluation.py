"""13.1 runner safety/reproducibility. No controller imports or live workers."""
from dataclasses import replace
import csv
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools/benchmarks'))
from evaluation import datasets,runner


class DatasetTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);self.root=Path(temporary.name)

    def test_same_recipe_identical_bytes_hash_size_and_multiple_kinds(self):
        a=self.root/'a';b=self.root/'b';a.mkdir();b.mkdir()
        for kind in datasets.KINDS:
            first=datasets.generate(a,kind,12,123,max_bytes=100000)
            second=datasets.generate(b,kind,12,123,max_bytes=100000)
            self.assertEqual(first,second);self.assertEqual((a/first['path']).read_bytes(),(b/second['path']).read_bytes())
            self.assertEqual(first['sha256'],datasets.digest(a/first['path']))
            self.assertEqual(first['size_bytes'],(a/first['path']).stat().st_size)
            with (a/first['path']).open() as stream:rows=list(csv.DictReader(stream))
            self.assertEqual(len(rows),12)
            self.assertEqual(len(rows[0]),65 if kind=='cnn' else 3 if kind=='ddp' else 2)

    def test_changed_seed_and_size_change_content_without_replacing_dataset(self):
        first=datasets.generate(self.root,'regression',8,123,max_bytes=10000)
        second=datasets.generate(self.root,'regression',16,123,max_bytes=10000)
        other=self.root/'other';other.mkdir()
        third=datasets.generate(other,'regression',8,124,max_bytes=10000)
        self.assertEqual(len({first['sha256'],second['sha256'],third['sha256']}),3)
        original=(self.root/first['path']).read_bytes()
        with self.assertRaises(FileExistsError):datasets.generate(self.root,'regression',8,999,max_bytes=10000)
        self.assertEqual((self.root/first['path']).read_bytes(),original)
        self.assertEqual(list(self.root.glob('*.part')),[])

    def test_byte_limit_aborts_without_publishing_partial_data(self):
        with self.assertRaises(ValueError):datasets.generate(self.root,'cnn',1000,123,max_bytes=200)
        self.assertEqual(list(self.root.iterdir()),[])


class RunnerTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);self.root=Path(temporary.name)
        self.settings=runner.Settings('acceptance',rows=(12,),workloads=('regression',))

    def test_plan_is_readonly_no_db_or_training_and_missing_pi_facts_are_unknown(self):
        with patch.object(runner,'make_root',side_effect=AssertionError('No write')),patch('sqlite3.connect',side_effect=AssertionError('No DB')):
            result=runner.plan(self.settings)
        self.assertEqual(result['scope'],'controller_local_smoke');self.assertIsNone(result['comparison_config'])
        self.assertEqual(result['scheduler_mode'],'not_exercised')
        self.assertEqual([w['worker_id'] for w in result['workers']],['pi01','pi02','pi03'])
        self.assertTrue(all(w['eligibility'] is None and w['pi_host_observed'] is None for w in result['workers']))
        self.assertFalse(result['local_host']['nfs_io_probed'])
        self.assertEqual(result['application_cache']['state'],'not_used')

    def test_invalid_identifiers_bounds_and_duplicates_are_refused(self):
        for changes in (dict(experiment_id='../job'),dict(experiment_id='/tmp'),dict(rows=(0,)),dict(rows=(12,12)),
                        dict(workloads=('arbitrary-code',)),dict(world_size=4),dict(timeout_seconds=0),dict(seed=-1),dict(steps=True)):
            with self.subTest(changes=changes),self.assertRaises(ValueError):replace(self.settings,**changes).validate()

    def test_existing_experiment_and_symlink_roots_are_not_overwritten(self):
        folder=runner.make_root(self.root,'same');(folder/'keep').write_text('user data')
        with self.assertRaises(FileExistsError):runner.make_root(self.root,'same')
        target=self.root/'target';target.mkdir();link=self.root/'link';link.symlink_to(target,target_is_directory=True)
        with self.assertRaises(OSError):runner.make_root(link,'refused')
        self.assertFalse((target/'refused').exists());self.assertEqual((folder/'keep').read_text(),'user data')
        with self.assertRaises(ValueError):runner.make_root(self.root/'..'/'bad','refused')

    def test_unsupported_report_filesystem_is_refused_before_creating_subdirectory(self):
        with patch.object(runner,'require_local_fd',side_effect=OSError('NFS')):
            with self.assertRaises(OSError):runner.make_root(self.root/'new','refused')
        self.assertFalse((self.root/'new').exists())

    def test_source_snapshot_preserves_exact_bytes_and_detects_source_changes(self):
        folder=self.root/'snapshot';folder.mkdir();source=runner.ROOT/'examples/checkpoint_project/train.py'
        key=str(source.relative_to(runner.ROOT));expected=datasets.digest(source)
        runner.snapshot(folder,{key:expected})
        self.assertEqual((folder/'source'/key).read_bytes(),source.read_bytes())
        other=self.root/'other';other.mkdir()
        with self.assertRaises(ValueError):runner.snapshot(other,{key:'0'*64})

    def test_source_symlinks_and_oversized_files_fail_before_snapshot(self):
        (self.root/'target').write_text('synthetic source')
        (self.root/'link').symlink_to(self.root/'target')
        (self.root/'large').write_bytes(b'x'*(1024*1024+1))
        with patch.object(runner,'ROOT',self.root):
            with self.assertRaises(OSError):runner.read_source('link')
            with self.assertRaises(ValueError):runner.read_source('large')

    def test_environment_removes_production_credentials_lease_resume_and_ddp_settings(self):
        with patch.dict(os.environ,DL_EXECUTION_LEASE_PATH='/live',DL_RESUME_CHECKPOINT='/live/save',
                        DL_WORKER_KEY='secret',RANK='7',MASTER_ADDR='live-pi',PYTHONPATH='/live/code'):
            env=runner.child_environment(self.root,self.root/'trial','a'*64)
        for key in ('DL_EXECUTION_LEASE_PATH','DL_RESUME_CHECKPOINT','DL_WORKER_KEY','RANK','MASTER_ADDR','PYTHONPATH'):
            self.assertNotIn(key,env)
        self.assertEqual(env['CUDA_VISIBLE_DEVICES'],'');self.assertEqual(env['GLOO_SOCKET_IFNAME'],'lo')

    def test_ddp_batch_size_supports_both_two_and_three_real_ranks(self):
        for world in (2,3):
            args=runner.trainer_arguments('ddp',replace(self.settings,world_size=world))
            self.assertEqual(int(args[args.index('--global-batch-size')+1])%world,0)

    def test_timeout_terminates_only_the_owned_process_group(self):
        trial=self.root/'trial';trial.mkdir()
        code,state=runner.execute([sys.executable,'-c','import time;time.sleep(30)'],trial,dict(os.environ),1)
        self.assertIsNone(code);self.assertEqual(state,'TIMEOUT')
        self.assertTrue((trial/'launcher.log').is_file())

    def test_missing_corrupt_or_unsynchronized_rank_cannot_be_success(self):
        self.assertFalse(runner.successful([],2))
        records=[dict(rank=r,runtime=dict(status='COMPLETED'),summary=dict(training_steps=1,rank=r,world_size=2,
                 parameters_synchronized=True,final_model_sha256='a'*64)) for r in range(2)]
        self.assertTrue(runner.successful(records,2));records[1]['summary']['final_model_sha256']='b'*64
        self.assertFalse(runner.successful(records,2));records[1]['summary']=None
        self.assertFalse(runner.successful(records,2))
        for record in records:record['summary']=dict(training_steps=1,rank=record['rank'],world_size=2,parameters_synchronized=True)
        self.assertFalse(runner.successful(records,2))

    def test_timeout_record_stays_timeout_with_unknown_training_time(self):
        with patch.object(runner,'execute',return_value=(None,'TIMEOUT')):
            folder,result=runner.run(self.settings,self.root)
        record=json.loads((folder/'trials.jsonl').read_text())
        self.assertEqual(result['status'],'FAILED');self.assertEqual(record['status'],'TIMEOUT')
        self.assertIsNone(record['exit_code']);self.assertIsNone(record['training_loop_seconds'])

    def test_launch_failure_is_retained_as_failed_csv_json_and_completion(self):
        with patch.object(runner,'execute',side_effect=OSError('synthetic launch failure')):
            folder,result=runner.run(self.settings,self.root)
        self.assertEqual(result['status'],'FAILED');self.assertEqual(result['completed_trials'],0)
        raw=json.loads((folder/'trials.jsonl').read_text());self.assertEqual(raw['status'],'FAILED')
        self.assertEqual(raw['error'],'OSError');self.assertIsNone(raw['training_loop_seconds'])
        with (folder/'trials.csv').open() as stream:flat=list(csv.DictReader(stream))
        self.assertEqual(flat[0]['status'],'FAILED');self.assertEqual(flat[0]['training_loop_seconds'],'')
        self.assertEqual(json.loads((folder/'completion.json').read_text())['status'],'FAILED')

    def test_dataset_generation_failure_is_not_reported_as_an_empty_success(self):
        settings=replace(self.settings,max_dataset_bytes=1)
        with self.assertRaises(ValueError):runner.run(settings,self.root)
        value=json.loads((self.root/'acceptance/completion.json').read_text())
        self.assertEqual(value['status'],'FAILED');self.assertEqual(value['trial_count'],0)

    def test_corrupt_result_and_nonfinite_json_are_rejected(self):
        rank=self.root/'ranks/rank-0/output';rank.mkdir(parents=True)
        (rank/'summary.json').write_text('{"value":NaN}')
        with self.assertRaises(ValueError):runner.collect(self.root,1)


class ActualTrainingTests(unittest.TestCase):
    def test_real_regression_and_cnn_use_unchanged_sources_and_keep_raw_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder,result=runner.run(runner.Settings('real-cpu',rows=(12,),workloads=('regression','cnn')),Path(temporary))
            self.assertEqual(result['status'],'COMPLETED')
            records=[json.loads(line) for line in (folder/'trials.jsonl').read_text().splitlines()]
            self.assertEqual(len(records),2)
            for record in records:
                self.assertEqual(record['status'],'COMPLETED');self.assertEqual(record['exit_code'],0)
                self.assertGreater(record['elapsed_ns'],0);self.assertGreater(record['training_loop_seconds'],0)
                self.assertEqual(record['ranks'][0]['summary']['training_steps'],2)
                self.assertEqual(record['ranks'][0]['runtime']['world_size'],1)
                self.assertTrue((folder/record['trial_id']/'ranks/rank-0/checkpoints/latest.dlckpt').is_file())
            self.assertEqual(records[0]['ranks'][0]['summary']['resumed_from'],0)
            original=ROOT/'examples/checkpoint_project/train.py'
            self.assertEqual((folder/'source/examples/checkpoint_project/train.py').read_bytes(),original.read_bytes())


if __name__=='__main__':unittest.main(verbosity=2)

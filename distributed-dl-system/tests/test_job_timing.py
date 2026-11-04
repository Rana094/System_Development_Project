"""Timing reports distinguish training duration, recovery downtime and DDP rank overlap."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'controller'))
from job_timing import build_report


class TimingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root/'results').mkdir()
        self.job = {'job_id':'timing-job','status':'COMPLETED','storage_path':str(self.root),
                    'created_at':100,'finished_at':180,
                    'job_config':json.dumps({'execution':{'mode':'single','workers':1}})}
        self.attempts = [dict(attempt_number=1,node_id='pi02',status='FAILED',started_at=110,finished_at=140),
                         dict(attempt_number=2,node_id='pi03',status='COMPLETED',started_at=150,finished_at=180)]

    def test_recovery_wall_time_includes_gap_and_preserves_final_attempt_scope(self):
        (self.root/'results/summary.json').write_text(json.dumps({'timing':{
            'training_loop_seconds':20,'training_program_seconds':25}}))
        report = build_report(self.job,self.attempts)
        self.assertEqual(report['full_training_wall_seconds'],70)
        self.assertEqual(report['submission_to_completion_seconds'],80)
        self.assertEqual(report['preparation_and_initial_queue_seconds'],10)
        self.assertEqual(report['final_attempt_training_loop_seconds'],20)
        self.assertEqual(report['final_attempt_training_program_seconds'],25)
        self.assertEqual(len(report['attempts']),2)

    def test_ddp_uses_slowest_rank_not_total(self):
        self.job['job_config'] = json.dumps({'execution':{'mode':'distributed','workers':3}})
        for rank,value in enumerate([10,11,12]):
            path=self.root/f'results/rank-{rank}';path.mkdir()
            (path/'summary.json').write_text(json.dumps({'timing':{
                'training_loop_seconds':value,'training_program_seconds':value+2}}))
        report=build_report(self.job,self.attempts)
        self.assertEqual(report['final_attempt_training_loop_seconds'],12)
        self.assertEqual(report['final_attempt_training_program_seconds'],14)
        (self.root/'results/rank-1/summary.json').unlink()
        self.assertIsNone(build_report(self.job,self.attempts)['final_attempt_training_loop_seconds'])

    def test_historical_and_unmeasured_values_are_not_invented(self):
        report=build_report(self.job,self.attempts)
        self.assertIsNone(report['final_attempt_training_loop_seconds'])
        self.assertIsNone(report['final_attempt_training_program_seconds'])
        self.assertEqual(report['full_training_wall_seconds'],70)
        self.job['status']='CANCELLED';self.job['finished_at']=105
        report=build_report(self.job,[])
        self.assertIsNone(report['full_training_wall_seconds'])
        self.assertEqual(report['submission_to_completion_seconds'],5)

    def test_legacy_ddp_loop_duration_supported_and_nonfinite_rejected(self):
        path=self.root/'results/summary.json'
        path.write_text(json.dumps({'training_seconds':3.5}))
        self.assertEqual(build_report(self.job,self.attempts)['final_attempt_training_loop_seconds'],3.5)
        path.write_text('{"timing":{"training_loop_seconds":NaN,"training_program_seconds":-5}}')
        report=build_report(self.job,self.attempts)
        self.assertIsNone(report['final_attempt_training_loop_seconds'])
        self.assertIsNone(report['final_attempt_training_program_seconds'])


if __name__=='__main__':unittest.main()

from copy import deepcopy
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from master.app.m6_metrics import MetricsCollector


class MeasurementTests(unittest.TestCase):
    def setUp(self):
        self.m = MetricsCollector()
        self.profile = {'weights_sha256':'same-model','implementation_sha256':'same-code','max_slots':1}
        self.meta = {'task_type':'convnext_infer','input_sha256':'same-input','shard_size':8,'correctness_version':'v1'}
        def run(jid,wids,start,end,units=240):
            self.m.register_job(jid,**self.meta)
            self.m.record_pool_job(jid,start,end,units,workers=wids,
                execution_profiles={w:deepcopy(self.profile) for w in wids},
                worker_devices={w:{'hostname':w,'gpu_uuid':w} for w in wids},
                failed_attempts=0,profile_changed=False,per_worker_units={w:units/len(wids) for w in wids})
        self.run = run
        run('warm-a',['a'],1,2,1)
        run('a',['a'],10,34)
        run('warm-b',['b'],35,37,1)
        run('b',['b'],40,52)
        run('pool',['a','b'],60,70)

    def benchmark(self):
        return self.m.create_benchmark(['a','b'],{'a':'a','b':'b'},'pool',{'a':'warm-a','b':'warm-b'})

    def test_official_formula_uses_independent_frozen_baselines(self):
        report = self.benchmark()
        self.assertEqual(report['per_device_throughput'],{'a':10,'b':20})
        self.assertEqual(report['p_pool'],24)
        self.assertEqual(report['eta_pool_pct'],80)
        self.assertEqual(report['speedup_S'],1.2)
        self.run('unrelated',['a'],80,81,10000)
        self.assertEqual(self.m.summary()['benchmark']['speedup_S'],1.2)

    def test_running_job_has_live_rate_but_no_fake_official_zero(self):
        m = MetricsCollector()
        with __import__('unittest').mock.patch('master.app.m6_metrics.time.time',return_value=20):
            summary = m.summary({'job_id':'live','created_at':10,'units_total':50,'per_worker_units':{'a':50}})
        self.assertEqual(summary['live']['throughput'],5)
        self.assertIsNone(summary['eta_pool_pct'])
        self.assertIsNone(summary['speedup_S'])
        self.assertIsNone(summary['p_pool'])

    def test_reject_incomparable_or_unstable_measurements(self):
        for key,value in [('input_sha256','different'),('task_type','image_thumb'),
                          ('shard_size',1),('correctness_version','v2'),
                          ('profile_changed',True)]:
            with self.subTest(key=key):
                previous = self.m._pool_jobs['a'][key]
                self.m._pool_jobs['a'][key] = value
                with self.assertRaises(ValueError): self.benchmark()
                self.m._pool_jobs['a'][key] = previous
        self.m._pool_jobs['pool']['execution_profiles']['a']['weights_sha256']='other'
        with self.assertRaises(ValueError): self.benchmark()

    def test_missing_baseline_or_nonindependent_run_is_rejected(self):
        with self.assertRaises(ValueError):
            self.m.create_benchmark(['a','b'],{'a':'a'},'pool',{'a':'warm-a','b':'warm-b'})
        self.m._pool_jobs['a']['workers']=['a','b']
        with self.assertRaises(ValueError): self.benchmark()

    def test_eta_above_100_is_not_clipped_or_silently_accepted(self):
        self.m._pool_jobs['pool']['p_pool']=40
        report = self.benchmark()
        self.assertAlmostEqual(report['eta_pool_pct'],133.3333333)
        self.assertEqual(report['status'],'needs_review')
        self.assertTrue(any('100%' in w for w in report['warnings']))

    def test_completed_retry_retains_wall_time_without_counting_failed_work(self):
        self.m._pool_jobs['a']['failed_attempts']=1
        report = self.benchmark()
        self.assertEqual(report['per_device_throughput']['a'],10)
        self.assertEqual(report['status'],'needs_review')
        self.assertTrue(any('retry time retained' in w for w in report['warnings']))

    def test_other_active_job_on_baseline_device_is_not_comparable(self):
        with self.assertRaises(ValueError):
            self.m.create_benchmark(['a','b'],{'a':'a','b':'b'},'pool',{'a':'warm-a','b':'warm-b'},
                observed_jobs=[{'job_id':'other','workers':['a'],'created_at':32,'finished_at':None}])

    def test_task_dedup_and_resource_window_missing_zero_and_clock_skew(self):
        self.assertTrue(self.m.record_attempt('a','t','att-1',5,1))
        self.assertFalse(self.m.record_attempt('a','t','att-2',5,1))
        for stamp,received,value in [(10000,11,0),(10001,12,100),(10001,12.5,100)]:
            self.m.record_sample('a',{'sampled_at':stamp,'interval_s':1,'gpu_pct':value,'cpu_pct':None},received)
        result = self.m.resource_window(['a','missing'],10,12)
        self.assertEqual(result['a']['avg_gpu_pct'],50)
        self.assertEqual(result['a']['gpu_pct_samples'],2)
        self.assertEqual(result['a']['gpu_pct_coverage'],1)
        self.assertIsNone(result['missing']['avg_gpu_pct'])
        self.assertEqual(result['missing']['gpu_pct_coverage'],0)

    def test_reports_survive_master_restart_with_frozen_measurements(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'reports.json'
            self.m._report_path = path
            saved = self.benchmark()
            reloaded = MetricsCollector(report_path=path).summary()['benchmark']
            self.assertEqual(reloaded,saved)

    def test_unresolvable_result_never_enters_valid_work_ledger(self):
        from shared.contracts import WorkerCapability
        from master.app.m2_registry import WorkerRegistry
        from master.app.m3_job import JobManager
        from master.app.m4_scheduler import TaskScheduler
        registry, jobs = WorkerRegistry(), JobManager()
        registry.register(WorkerCapability('a','host-a',2,8,supported_task_types=['image_thumb']))
        job = jobs.create_job('image_thumb',['input'])
        scheduler = TaskScheduler(jobs,registry)
        assignment = scheduler.assign_task('a',1)
        with patch('master.app.m4_scheduler._validate',lambda *_:999), \
             patch('master.app.m4_scheduler._resolve_output',lambda *_:None):
            reply = scheduler.commit_result(assignment['attempt']['attempt_id'],'missing',999,None)
        self.assertTrue(reply['failed'])
        self.assertEqual(jobs.ledger(job.job_id)['units_total'],0)


if __name__=='__main__': unittest.main()

from copy import deepcopy
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from dormpool.src.master.app.m6_metrics import MetricsCollector


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
        with __import__('unittest').mock.patch('dormpool.src.master.app.m6_metrics.time.time',return_value=20):
            summary = m.summary({'job_id':'live','created_at':10,'units_total':50,'per_worker_units':{'a':50}})
        self.assertEqual(summary['live']['throughput'],5)
        self.assertIsNone(summary['eta_pool_pct'])
        self.assertIsNone(summary['speedup_S'])
        self.assertIsNone(summary['p_pool'])

    def test_choices_are_latest_plus_five_timestamped_history_records(self):
        m = MetricsCollector()
        for index in [2, 7, 0, 5, 6, 3, 1, 4]:
            key = f'bench-{index}'
            m._benchmarks[key] = dict(benchmark_id=key, name='same name', created_at=index,
                                     eta_pool_pct=80, speedup_S=1.2)
        summary = m.summary()
        self.assertEqual(summary['benchmark']['benchmark_id'], 'bench-7')
        choices = summary['benchmark_choices']
        self.assertEqual([r['benchmark_id'] for r in choices],
                         [f'bench-{i}' for i in range(7, 1, -1)])
        self.assertEqual([r['created_at'] for r in choices], list(range(7, 1, -1)))
        self.assertEqual(len(m.benchmarks()), 8)  # Limit choices, never delete stored reports.
        self.assertEqual(m.summary(benchmark_id='bench-0')['benchmark']['created_at'], 0)

    def test_automatic_single_machine_measurement_is_always_available(self):
        m = MetricsCollector()
        m.register_job('single', **self.meta)
        m.record_pool_job('single', 10, 22, 120,
                          workers=['gpu-1'],
                          execution_profiles={'gpu-1': deepcopy(self.profile)},
                          worker_devices={'gpu-1': {'hostname': 'host', 'gpu_uuid': 'gpu'}},
                          failed_attempts=0, profile_changed=False,
                          per_worker_units={'gpu-1': 120}, succeeded_attempts=1)
        report = m.record_automatic_benchmark('single')
        self.assertEqual(report['status'], 'needs_review')  # legacy Job-window fallback
        self.assertEqual(report['per_device_throughput'], {'gpu-1': 10})
        self.assertEqual(report['p_pool'], 10)
        self.assertEqual(report['eta_pool_pct'], 100.0)
        self.assertEqual(report['speedup_S'], 1.0)
        self.assertEqual(m.summary()['benchmark']['benchmark_id'], report['benchmark_id'])

    def test_automatic_pool_records_each_worker_and_aggregates_without_baselines(self):
        m = MetricsCollector()
        m.register_job('pool-auto', **self.meta)
        m.record_pool_job('pool-auto', 10, 22, 120,
                          workers=['gpu-1', 'gpu-2'],
                          execution_profiles={'gpu-1': deepcopy(self.profile), 'gpu-2': deepcopy(self.profile)},
                          worker_devices={'gpu-1': {'hostname': 'h1', 'gpu_uuid': 'g1'},
                                          'gpu-2': {'hostname': 'h2', 'gpu_uuid': 'g2'}},
                          failed_attempts=0, profile_changed=False,
                          per_worker_units={'gpu-1': 60, 'gpu-2': 60}, succeeded_attempts=2,
                          per_worker_windows={'gpu-1': {'started_at': 11, 'finished_at': 21},
                                              'gpu-2': {'started_at': 12, 'finished_at': 20}})
        report = m.record_automatic_benchmark('pool-auto')
        self.assertEqual(report['status'], 'recorded')
        self.assertEqual(report['p_pool'], 10)
        self.assertEqual(report['per_device_throughput'], {'gpu-1': 6, 'gpu-2': 7.5})
        self.assertEqual(report['device_metrics']['gpu-2']['processing_s'], 8)
        self.assertAlmostEqual(report['eta_pool_pct'], 10 / 13.5 * 100)
        self.assertAlmostEqual(report['speedup_S'], 10 / 7.5)
        self.assertEqual(report['baseline_jobs'], {})
        self.assertEqual(report['rate_source'], 'observed_worker_window')

    def test_single_worker_uses_actual_processing_time_instead_of_forcing_one(self):
        self.m._pool_jobs['a']['per_worker_windows'] = {
            'a': {'started_at': 12, 'finished_at': 32}}
        report = self.m.record_automatic_benchmark('a')
        self.assertEqual(report['device_metrics']['a']['processing_s'], 20)
        self.assertEqual(report['per_device_throughput']['a'], 12)
        self.assertEqual(report['p_pool'], 10)
        self.assertAlmostEqual(report['speedup_S'], 10 / 12)
        self.assertAlmostEqual(report['eta_pool_pct'], 10 / 12 * 100)

    def test_legacy_automatic_pool_is_readable_without_missing_device_rows(self):
        legacy = dict(benchmark_id='old', name='old report', created_at=100,
                      mode='automatic_pool', pool_job='pool', participants=['a', 'b'],
                      measurements={'pool': deepcopy(self.m._pool_jobs['pool'])},
                      per_device_throughput={'a': 10}, p_pool=24,
                      eta_pool_pct=None, speedup_S=None)
        self.m._benchmarks['old'] = legacy
        report = self.m.summary(benchmark_id='old')['benchmark']
        self.assertEqual(set(report['device_metrics']), {'a', 'b'})
        self.assertTrue(all(d['time_source'] == 'job_window' for d in report['device_metrics'].values()))
        self.assertIsNotNone(report['eta_pool_pct'])
        self.assertEqual(legacy['per_device_throughput'], {'a': 10})

    def test_device_window_counts_retries_and_does_not_sum_concurrent_attempts(self):
        from dormpool.src.master.app.m3_job import JobManager
        from dormpool.src.shared.contracts import TaskStatus
        jobs = JobManager()
        job = jobs.create_job('image_thumb', ['1', '2'])
        first, second = jobs.tasks_of(job.job_id)
        for task in (first, second):
            task.status = TaskStatus.SUCCEEDED
        jobs.add_attempt(first.task_id, dict(worker_id='w', status='failed', started_at=10,
                                            finished_at=14, units_done=0))
        jobs.add_attempt(first.task_id, dict(worker_id='w', status='succeeded', started_at=14,
                                            finished_at=20, units_done=50))
        jobs.add_attempt(second.task_id, dict(worker_id='w', status='succeeded', started_at=15,
                                             finished_at=22, units_done=25))
        ledger = jobs.ledger(job.job_id)
        self.assertEqual(ledger['units_total'], 75)
        self.assertEqual(ledger['per_worker_windows']['w'], {'started_at': 10, 'finished_at': 22})
        ledger['wall_s'] = 20
        device = self.m.device_measurements(ledger)['w']
        self.assertEqual(device['processing_s'], 12)
        self.assertEqual(device['p_i'], 75 / 12)

    def test_each_job_retains_report_and_does_not_overwrite_official_record(self):
        official = self.benchmark()
        self.m.record_automatic_benchmark('a')
        self.m.record_automatic_benchmark('pool')
        reports = self.m.benchmarks()
        self.assertEqual(len(reports), 3)
        self.assertEqual(reports[0], official)
        with tempfile.TemporaryDirectory() as folder:
            self.m._report_path = Path(folder) / 'reports.json'
            self.m._save_benchmarks_locked()
            loaded = MetricsCollector(self.m._report_path).summary()['benchmark']
            self.assertEqual(loaded['device_metrics']['a']['units_done'], 120)
            self.assertIsNotNone(loaded['speedup_S'])

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
        from dormpool.src.shared.contracts import WorkerCapability
        from dormpool.src.master.app.m2_registry import WorkerRegistry
        from dormpool.src.master.app.m3_job import JobManager
        from dormpool.src.master.app.m4_scheduler import TaskScheduler
        registry, jobs = WorkerRegistry(), JobManager()
        registry.register(WorkerCapability('a','host-a',2,8,supported_task_types=['image_thumb']))
        job = jobs.create_job('image_thumb',['input'])
        scheduler = TaskScheduler(jobs,registry)
        assignment = scheduler.assign_task('a',1)
        with patch('dormpool.src.master.app.m4_scheduler._validate',lambda *_:999), \
             patch('dormpool.src.master.app.m4_scheduler._resolve_output',lambda *_:None):
            reply = scheduler.commit_result(assignment['attempt']['attempt_id'],'missing',999,None)
        self.assertTrue(reply['failed'])
        self.assertEqual(jobs.ledger(job.job_id)['units_total'],0)


if __name__=='__main__': unittest.main()

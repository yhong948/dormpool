"""Job controls preserve completed work and reject results after deletion."""
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import zipfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dormpool.src.master.app import m3_job
from dormpool.src.master.app import m4_scheduler
from dormpool.src.master.app.m2_registry import WorkerRegistry
from dormpool.src.master.app.m3_job import JobManager
from dormpool.src.master.app.m4_scheduler import TaskScheduler
from dormpool.src.master.app.m5_data import DataManager
from dormpool.src.shared.contracts import JobStatus, WorkerCapability


class JobControlTests(unittest.TestCase):
    def setUp(self):
        self.jobs = JobManager()
        self.registry = WorkerRegistry()
        self.registry.register(WorkerCapability('worker', 'test', 1, 8))
        self.scheduler = TaskScheduler(self.jobs, self.registry)
        for module, name in ((m3_job, '_on_job_completed'), (m4_scheduler, '_validate'),
                             (m4_scheduler, '_on_success'), (m4_scheduler, '_on_attempt_created')):
            mock = patch.object(module, name, None)
            mock.start()
            self.addCleanup(mock.stop)

    def claim(self):
        return self.scheduler.assign_task('worker', 1)

    def finish(self, assignment):
        return self.scheduler.commit_result(assignment['attempt']['attempt_id'], 'result.zip', 1, None)

    def test_pause_pending_job_and_resume_idempotently(self):
        job = self.jobs.create_job('image_thumb', ['a.zip'])
        for _ in range(2):
            self.scheduler.set_job_paused(job.job_id, True)
            self.assertIsNone(self.claim())
        for _ in range(2):
            self.scheduler.set_job_paused(job.job_id, False)
        self.assertEqual(job.status, JobStatus.QUEUED)
        self.assertIsNotNone(self.claim())
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_pause_preserves_inflight_result_and_other_jobs_still_run(self):
        job = self.jobs.create_job('image_thumb', ['a.zip', 'b.zip'])
        first = self.claim()
        self.scheduler.set_job_paused(job.job_id, True)
        self.assertTrue(self.finish(first)['committed'])
        self.assertEqual(job.done_tasks, 1)
        self.assertEqual(job.status, JobStatus.PAUSED)
        self.assertIsNone(self.claim())
        other = self.jobs.create_job('image_thumb', ['c.zip'])
        self.assertEqual(self.claim()['task']['job_id'], other.job_id)
        self.scheduler.set_job_paused(job.job_id, False)
        self.assertTrue(self.finish(self.claim())['committed'])
        self.assertEqual(job.status, JobStatus.COMPLETED)

    def test_last_inflight_task_can_complete_a_paused_job(self):
        job = self.jobs.create_job('image_thumb', ['a.zip'])
        assignment = self.claim()
        self.scheduler.set_job_paused(job.job_id, True)
        self.finish(assignment)
        self.assertEqual(job.status, JobStatus.COMPLETED)
        with self.assertRaises(ValueError):
            self.scheduler.set_job_paused(job.job_id, False)

    def test_delete_closes_attempts_and_late_results_cannot_restore_job(self):
        job = self.jobs.create_job('image_thumb', ['a.zip', 'b.zip'])
        assignment = self.claim()
        attempt_id = assignment['attempt']['attempt_id']
        self.scheduler.delete_job(job.job_id)
        self.assertIsNone(self.jobs.job_view(job.job_id))
        self.assertEqual(self.jobs.list_jobs(), [])
        self.assertEqual(self.scheduler.open_attempts(), [])
        self.assertIsNone(self.claim())
        self.assertTrue(self.finish(assignment)['discarded'])
        self.assertIn('error', self.scheduler.renew_lease(attempt_id))
        self.scheduler.reclaim_worker('worker')
        self.scheduler.scan_expired_leases()
        self.assertEqual(self.jobs.list_jobs(), [])
        for action in (lambda: self.scheduler.delete_job(job.job_id),
                       lambda: self.scheduler.set_job_paused(job.job_id, True)):
            with self.assertRaises(KeyError):
                action()

    def test_deleted_files_stay_deleted_and_neighbor_is_preserved(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, DORMPOOL_DATA=folder):
            data = DataManager()
            data.save_input('job-a', 'input.zip', b'input')
            data.save_task_output('job-a', 'task-a', b'output')
            data.save_input('job-b', 'input.zip', b'neighbor')
            data.delete_job('job-a')
            self.assertFalse((Path(folder) / 'inputs/job-a').exists())
            self.assertFalse((Path(folder) / 'outputs/job-a').exists())
            self.assertTrue((Path(folder) / 'inputs/job-b/input.zip').exists())
            with self.assertRaises(ValueError):
                data.save_task_output('job-a', 'task-a', b'late output')
            with self.assertRaises(ValueError):
                data.delete_job('../job-b')


class JobControlHTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.data_root = Path(cls.directory.name)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        cls.base = f'http://127.0.0.1:{port}'
        cls.log = (cls.data_root / 'master.log').open('w')
        cls.process = subprocess.Popen(
            [sys.executable, '-m', 'uvicorn', 'src.master.main:app', '--host', '127.0.0.1', '--port', str(port)],
            cwd=ROOT, env={**os.environ, 'DORMPOOL_DATA': str(cls.data_root), 'SHARD_SIZE': '1'},
            stdout=cls.log, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        cls.addClassCleanup(cls.cleanup_server)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                if cls.request('/ping')[0] == 200:
                    return
            except OSError:
                time.sleep(0.05)
        raise AssertionError('test Master did not start')

    @classmethod
    def cleanup_server(cls):
        cls.process.terminate()
        cls.process.wait(timeout=10)
        cls.log.close()
        cls.directory.cleanup()

    @classmethod
    def request(cls, path, method='GET', body=None, content_type='application/json'):
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        request = Request(cls.base + path, data=body, method=method,
                          headers={'Content-Type': content_type})
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as error:
            response = error
        with response:
            raw = response.read()
            return response.code, json.loads(raw) if raw else None

    def test_http_pause_resume_delete_and_late_upload(self):
        source = io.BytesIO()
        with zipfile.ZipFile(source, 'w') as archive:
            archive.writestr('one.png', b'test image')
        boundary = 'dormpool-job-controls'
        payload = (f'--{boundary}\r\nContent-Disposition: form-data; name="task_type"\r\n\r\nimage_thumb\r\n'
                   f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="input.zip"\r\n'
                   'Content-Type: application/zip\r\n\r\n').encode() + source.getvalue() + f'\r\n--{boundary}--\r\n'.encode()
        status, job = self.request('/jobs', 'POST', payload, f'multipart/form-data; boundary={boundary}')
        self.assertEqual(status, 200)
        path = '/jobs/' + job['job_id']
        self.assertEqual(self.request(path + '/pause', 'POST')[1]['status'], 'paused')
        self.request('/workers/register', 'POST', dict(worker_id='http-worker', hostname='test', cpu_cores=1, ram_gb=8))
        self.assertEqual(self.request('/workers/http-worker/claim', 'POST', {'free_slots': 1})[0], 204)
        self.assertEqual(self.request(path + '/resume', 'POST')[1]['status'], 'queued')
        _, assignment = self.request('/workers/http-worker/claim', 'POST', {'free_slots': 1})
        self.assertIn('attempt', assignment)
        self.assertEqual(self.request(path + '/pause', 'POST')[1]['status'], 'paused')
        self.assertEqual(self.request(path, 'DELETE')[0], 200)
        self.assertEqual(self.request(path)[0], 404)
        self.assertEqual(self.request(path + '/pause', 'POST')[0], 404)
        self.assertEqual(self.request(path + '/resume', 'POST')[0], 404)
        self.assertEqual(self.request(path, 'DELETE')[0], 404)
        attempt = assignment['attempt']['attempt_id']
        self.assertTrue(self.request('/attempts/' + attempt + '/commit', 'POST', {'output_ref': 'late.zip', 'units_done': 1})[1]['discarded'])
        task_id = assignment['task']['task_id']
        upload = '/data/upload/' + job['job_id'] + '/' + task_id
        self.assertEqual(self.request(upload, 'POST', payload, f'multipart/form-data; boundary={boundary}')[0], 404)
        self.assertFalse((self.data_root / 'inputs' / job['job_id']).exists())
        self.assertFalse((self.data_root / 'outputs' / job['job_id']).exists())
        self.assertEqual(self.request('/jobs')[1]['jobs'], [])


if __name__ == '__main__':
    unittest.main()

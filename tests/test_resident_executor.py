import io
import json
import logging
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from worker.agent.executor_client import ResidentExecutor, ExecutorFailure
from worker.agent.w1_runtime import AgentConfig
from worker.agent.w5_exec import ExecutionManager
from worker.agent.w3_monitor import ResourceMonitor


class ResidentTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.path = Path(self.folder.name)
        self.client = ResidentExecutor([sys.executable, str(ROOT/'tests/fixtures/executor_peer.py')], 5)

    def tearDown(self):
        self.client.close()
        self.folder.cleanup()

    def execute(self, mode='good', timeout=2):
        return self.client.execute({'task_id':'t','task_type':'image_thumb'},
            {'attempt_id':'a'}, self.path/mode, self.path/'out', timeout)

    def test_reuse_and_ordinary_error_keeps_process(self):
        pid = self.client.ensure_ready()['pid']
        for _ in range(10):
            self.assertEqual(self.execute()['pid'], pid)
        self.assertFalse(self.execute('bad')['ok'])
        self.assertFalse((self.path/'out').exists(), 'stale output survived dispatch')
        self.assertEqual(self.execute()['pid'], pid)
        self.assertEqual(self.client.status()['generation'],1)

    def test_stderr_drain_does_not_block_protocol(self):
        with patch.object(logging.getLogger('executor'), 'info'):
            self.assertTrue(self.execute('noise')['ok'])

    def test_crash_timeout_bad_protocol_reap_and_restart(self):
        for mode in ('crash','hang','invalid','mismatch'):
            with self.subTest(mode=mode):
                self.client.ensure_ready()
                old = self.client._proc
                with self.assertRaises(ExecutorFailure):
                    self.execute(mode, timeout=0.15)
                self.assertIsNotNone(old.poll(), 'old child not reaped')
                self.assertNotEqual(self.execute()['pid'], old.pid)

    def test_fatal_response_recycles_process(self):
        old = self.client.ensure_ready()['pid']
        self.assertTrue(self.execute('fatal')['fatal'])
        self.assertIsNone(self.client.status()['pid'])
        self.assertNotEqual(self.execute()['pid'], old)

    def test_close_interrupts_hung_request_and_prevents_restart(self):
        self.client.ensure_ready()
        old = self.client._proc
        errors = []
        def work():
            try:
                self.execute('hang',30)
            except ExecutorFailure as exc:
                errors.append(str(exc))
        thread = threading.Thread(target=work)
        thread.start()
        time.sleep(0.1)
        self.client.close()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertTrue(errors)
        self.assertIsNotNone(old.poll())
        with self.assertRaises(ExecutorFailure):
            self.client.ensure_ready()

    def test_renewal_covers_download_and_cleanup(self):
        manager = ExecutionManager(AgentConfig(data_dir=self.path))
        manager.client = self.client
        download = threading.Event()
        renew = threading.Event()
        commits = []
        def fetch(*_):
            download.wait(3)
            return False
        agent = SimpleNamespace(monitor=ResourceMonitor(1), _work_event=threading.Event(),
            connector=SimpleNamespace(fetch_input=fetch, renew_lease=lambda *_: renew.set(),
                                      commit_result=lambda *a: commits.append(a)))
        manager.run_task({'task_id':'t'}, {'attempt_id':'a','lease_expires':time.time()-1}, agent)
        self.assertTrue(renew.wait(1), 'download not covered by renewal')
        download.set()
        manager.close(timeout=3)
        self.assertEqual(manager.active_count(),0)
        self.assertEqual(agent.monitor.busy_slots(),0)
        self.assertEqual(len(commits),1)
        self.assertIn('input fetch failed',commits[0][-1])

    def test_manager_timeout_commits_failure_then_recovers_next_attempt(self):
        manager = ExecutionManager(AgentConfig(data_dir=self.path))
        manager.client = self.client
        manager.task_timeout = 0.15
        commits = []
        agent = SimpleNamespace(monitor=ResourceMonitor(1), _work_event=threading.Event(),
            connector=SimpleNamespace(fetch_input=lambda *_: True, renew_lease=lambda *_: True,
                upload_output=lambda *_: 'output-ref',
                commit_result=lambda *a: commits.append(a) or {'committed':True}))
        old_pid = manager.prepare()['pid']
        manager.run_task({'task_id':'t','task_type':'hang'}, {'attempt_id':'failed'}, agent)
        self.assertTrue(agent._work_event.wait(3))
        self.assertIn('timeout',commits[0][-1])
        self.assertEqual(agent.monitor.busy_slots(),0)
        agent._work_event.clear()
        manager.run_task({'task_id':'t','task_type':'image_thumb'}, {'attempt_id':'retry'}, agent)
        self.assertTrue(agent._work_event.wait(3))
        self.assertIsNone(commits[-1][-1])
        self.assertNotEqual(self.client.status()['pid'],old_pid)
        self.assertEqual(len(commits),2, 'manager retried internally')
        manager.close()

    def test_graceful_drain_waits_for_accepted_task(self):
        manager = ExecutionManager(AgentConfig(data_dir=self.path))
        manager.client = self.client
        started, finish = threading.Event(), threading.Event()
        def guarded(*_):
            started.set()
            finish.wait(3)
        with patch.object(manager,'_run_guarded',side_effect=guarded):
            manager.run_task({'task_id':'t'}, {'attempt_id':'a'})
            self.assertTrue(started.wait(1))
            closer = threading.Thread(target=lambda: manager.close(timeout=2))
            closer.start()
            time.sleep(0.05)
            self.assertTrue(closer.is_alive())
            finish.set()
            closer.join(2)
            self.assertFalse(closer.is_alive())
            self.assertEqual(manager.active_count(),0)

    def test_startup_timeout_reaps_process(self):
        self.client.close()
        self.client = ResidentExecutor([sys.executable,'-c','import time;time.sleep(30)'],0.1)
        with self.assertRaises(ExecutorFailure):
            self.client.ensure_ready()
        self.assertIsNone(self.client.status()['launcher_pid'])


class ServiceTests(unittest.TestCase):
    def test_cuda_error_is_fatal_and_stops_service(self):
        from executor.service import serve
        request = dict(type='execute',request_id='r',task_id='t',attempt_id='a',
                       task_type='toy',input_path='',output_path='')
        output = io.StringIO()
        def handler(*_):
            raise RuntimeError('CUDA context corrupted')
        with patch('executor.service.initialize',return_value={'type':'ready','supported_task_types':['toy']}), \
             patch('sys.stdin',io.StringIO(json.dumps(request)+'\n')), \
             patch('sys.stdout',output), patch('sys.stderr',io.StringIO()):
            self.assertEqual(serve({'toy':handler}),2)
        self.assertTrue(json.loads(output.getvalue().splitlines()[-1])['fatal'])

    def test_protocol_stdout_and_ids_and_recoverable_input(self):
        from executor.service import serve
        requests = [dict(type='execute',request_id=str(n),task_id='t',attempt_id=str(n),
                         task_type='toy',input_path='',output_path='') for n in range(3)]
        source = io.StringIO(''.join(json.dumps(r)+'\n' for r in requests))
        output = io.StringIO()
        calls = []
        def handler(*_):
            print('handler noise must go to stderr')
            calls.append(1)
            if len(calls)==2:
                raise ValueError('bad input')
            return 1
        with patch('executor.service.initialize', return_value={'type':'ready','supported_task_types':['toy']}), \
             patch('sys.stdin',source), patch('sys.stdout',output), patch('sys.stderr',io.StringIO()):
            self.assertEqual(serve({'toy':handler}),0)
        messages = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([r['ok'] for r in messages[1:]],[True,False,True])
        self.assertEqual([r['attempt_id'] for r in messages[1:]],['0','1','2'])


if __name__ == '__main__':
    unittest.main()

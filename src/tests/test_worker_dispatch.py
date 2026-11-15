"""Exercise capacity and completion wakeups without a network or GPU."""
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dormpool.src.worker.agent.w1_runtime import AgentConfig, AgentRuntime
from dormpool.src.worker.agent.w3_monitor import ResourceMonitor
from dormpool.src.worker.agent.w4_contrib import ContributionController
from dormpool.src.worker.agent.w5_exec import ExecutionManager


class DispatchTests(unittest.TestCase):
    def test_fills_slots_and_refills_on_completion_without_poll_delay(self):
        with tempfile.TemporaryDirectory() as directory:
            agent = AgentRuntime(AgentConfig(max_slots=2, data_dir=directory))
            agent.monitor = ResourceMonitor(2)
            agent.contrib = ContributionController(agent.monitor)
            agent.exec_mgr = ExecutionManager(agent.config)
            claimed = []
            gates = [threading.Event() for _ in range(3)]
            started = [threading.Event() for _ in range(3)]
            full = threading.Event()
            third = threading.Event()

            def claim():
                self.assertGreater(agent.monitor.free_slots(), 0)
                index = len(claimed)
                if index == 3:
                    return None
                claimed.append(time.monotonic())
                if index == 1:
                    full.set()
                if index == 2:
                    third.set()
                return {'task': {'task_id': str(index)},
                        'attempt': {'attempt_id': str(index)}}

            def execute(task, *_):
                index = int(task['task_id'])
                started[index].set()
                gates[index].wait(10)

            agent.connector = SimpleNamespace(claim_task=claim)
            with patch.object(agent.exec_mgr, '_run_guarded', side_effect=execute):
                loop = threading.Thread(target=agent._task_loop)
                loop.start()
                try:
                    self.assertTrue(full.wait(2), 'did not fill both slots')
                    self.assertTrue(started[1].wait(2))
                    self.assertEqual(agent.monitor.busy_slots(), 2)
                    self.assertLess(claimed[1] - claimed[0], 0.15,
                                    'fixed inter-claim sleep remains')
                    self.assertFalse(third.wait(0.05), 'exceeded capacity')
                    # A long idle timeout proves the completion signal wakes us;
                    # shrinking the polling constant alone cannot pass this.
                    with patch('worker.agent.w1_runtime.POLL_IDLE', 30):
                        agent._work_event.set()
                        time.sleep(0.05)
                        gates[0].set()
                        self.assertTrue(third.wait(1), 'completion did not wake claim loop')
                    self.assertLessEqual(agent.monitor.busy_slots(), 2)
                finally:
                    agent.shutdown()
                    for gate in gates:
                        gate.set()
                    loop.join(2)
                    self.assertFalse(loop.is_alive())
                    deadline = time.monotonic() + 2
                    while agent.monitor.busy_slots() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertEqual(agent.monitor.busy_slots(), 0)


if __name__ == '__main__':
    unittest.main()

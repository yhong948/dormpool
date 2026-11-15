"""Jobs wait for compatible workers; local model discovery honors overrides."""

import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dormpool.src.executor.workload import convnext_infer
from dormpool.src.master.app.m2_registry import WorkerRegistry
from dormpool.src.master.app.m3_job import JobManager
from dormpool.src.master.app.m4_scheduler import TaskScheduler
from dormpool.src.shared.contracts import JobStatus, WorkerCapability


class ReadinessTests(unittest.TestCase):
    def test_job_waits_until_compatible_worker_claims_it(self):
        jobs = JobManager()
        registry = WorkerRegistry()
        scheduler = TaskScheduler(jobs, registry)
        job = jobs.create_job("convnext_infer", ["inputs/test.zip"])
        capability = WorkerCapability(
            worker_id="gpu", hostname="test", cpu_cores=1, ram_gb=8,
            gpu_name="test GPU", supported_task_types=["image_thumb_gpu"],
        )
        registry.register(capability)
        self.assertIsNone(scheduler.assign_task("gpu", 1))
        self.assertEqual(job.status, JobStatus.QUEUED)
        capability.supported_task_types.append("convnext_infer")
        assignment = scheduler.assign_task("gpu", 1)
        self.assertIsNotNone(assignment)
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_models_checkpoint_and_legacy_data_checkpoint_are_discovered(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(convnext_infer, "REPO_ROOT", directory), patch.dict(os.environ):
                os.environ.pop(convnext_infer.WEIGHTS_ENV, None)
                legacy = root / "data" / "convnext_small-0c510722.pth"
                legacy.parent.mkdir()
                legacy.touch()
                self.assertEqual(Path(convnext_infer._weights_path()), legacy)
                model = root / "models" / "convnext_small-0c510722.pth"
                model.parent.mkdir()
                model.touch()
                self.assertEqual(Path(convnext_infer._weights_path()), model)
                os.environ[convnext_infer.WEIGHTS_ENV] = "custom.pth"
                self.assertEqual(Path(convnext_infer._weights_path()), root / "custom.pth")


if __name__ == "__main__":
    unittest.main()

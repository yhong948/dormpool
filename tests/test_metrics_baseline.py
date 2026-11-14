import tempfile
import unittest
from pathlib import Path

from master.app.m6_metrics import MetricsCollector


class BaselineMetricTests(unittest.TestCase):
    def test_single_baseline_also_populates_single_node_indicators(self):
        metrics = MetricsCollector()
        profile = {"dataset_sha256": "abc", "shard_size": 1,
                   "correctness": "validated-output-v1"}
        config = {"hostname": "node", "gpu_name": "gpu", "max_slots": 1,
                  "runtime": {"executor_version": "1"}}
        metrics.record_measurement("baseline", "baseline", "task", 1, 3,
                                   3, 3, profile, {"node": config}, [])
        summary = metrics.summary()
        self.assertEqual(summary["p_pool_source"], "baseline_wall_clock")
        self.assertEqual(summary["per_device_throughput"], {"node": 1.5})
        self.assertEqual(summary["eta_pool_pct"], 100.0)
        self.assertEqual(summary["speedup_S"], 1.0)

    def test_pool_compares_only_to_matching_independent_baseline(self):
        with tempfile.TemporaryDirectory() as folder:
            metrics = MetricsCollector(Path(folder) / "measurements.json")
            profile = {"dataset_sha256": "abc", "shard_size": 1,
                       "correctness": "validated-output-v1"}
            config = {"hostname": "node", "gpu_name": "gpu", "max_slots": 1,
                      "runtime": {"executor_version": "1"}}
            metrics.record_measurement("baseline", "baseline", "task", 1, 3,
                                       3, 3, profile, {"node": config}, [])
            metrics.record_measurement("pool", "pool", "task", 10, 15,
                                       10, 10, profile, {"node": config}, [])
            summary = metrics.summary()
            self.assertTrue(summary["baseline_comparable"])
            self.assertEqual(summary["per_device_source"], "independent_baseline")
            self.assertEqual(summary["per_device_throughput"], {"node": 1.5})
            self.assertEqual(summary["p_pool"], 2.0)
            self.assertEqual(summary["speedup_S"], 1.3333)

    def test_missing_or_changed_baseline_is_explicitly_incomparable(self):
        metrics = MetricsCollector()
        profile = {"dataset_sha256": "abc", "shard_size": 1,
                   "correctness": "validated-output-v1"}
        config = {"hostname": "node", "gpu_name": "gpu", "max_slots": 1,
                  "runtime": {"executor_version": "1"}}
        metrics.record_measurement("pool", "pool", "task", 10, 15,
                                   10, 10, profile, {"node": config}, [])
        summary = metrics.summary()
        self.assertFalse(summary["baseline_comparable"])
        self.assertIsNone(summary["speedup_S"])
        self.assertIn("missing_baseline:node", summary["baseline_issues"])


if __name__ == "__main__":
    unittest.main()

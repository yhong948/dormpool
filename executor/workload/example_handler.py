"""
executor/workload/example_handler.py  —  C2 example workload stub
==================================================================
Person C: copy this file, rename it for your workload type, implement run().
Register the handler in executor/run.py TASK_REGISTRY.
"""


def run(input_path: str, output_path: str) -> int:
    """
    Execute one Task shard.

    Args:
        input_path:  local path to the input shard file (downloaded by Worker)
        output_path: local path where this handler should write its result

    Returns:
        units_done: count of valid work units completed (e.g. images processed)

    Raises any exception on failure — the caller (run.py) will catch and exit 1.

    C3 note: output must be deterministic for a given input so the Master can
    verify correctness with a known reference.
    """
    # TODO (Person C): replace with real computation
    raise NotImplementedError("Implement workload handler before running tasks")

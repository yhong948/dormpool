"""
executor/workload/c3_validation.py  —  C3 Validation & Accounting
==================================================================
Person C owns this file.

Responsibility:
  - Verify that an executor output is correct before Master confirms the Task
  - Count valid work units (images, frames, tokens, etc.)
  - Prevent duplicate counting across Attempts

Called by Master M4/M5 after receiving a commit, OR by the executor itself
before writing its output (preferred for fast fail).
"""

from typing import Any, Optional


class ValidationError(Exception):
    pass


class ResultValidator:
    """
    Base class for workload-specific validators.
    Person C: subclass for each task_type and register in VALIDATORS below.
    """

    def validate(self, output_path: str, task_meta: dict) -> int:
        """
        Verify output file at output_path.

        Args:
            output_path: path to the executor output file
            task_meta:   Task dict (task_id, task_type, input_ref, resource_req)

        Returns:
            Confirmed units_done (may differ from executor's self-reported value)

        Raises:
            ValidationError if output is missing, corrupt, or incorrect
        """
        raise NotImplementedError


# ── Registry ──────────────────────────────────────────────────────────────────
# Map task_type → ResultValidator instance
# TODO (Person C): populate once workload is defined
#   from executor.workload.image_resize_validator import ImageResizeValidator
#   VALIDATORS = {"image_resize": ImageResizeValidator()}

VALIDATORS: dict[str, ResultValidator] = {}


def validate_result(output_path: str, task_meta: dict) -> int:
    """
    Validate a task result.  Returns units_done or raises ValidationError.
    If no validator is registered for the task_type, trusts executor's report.
    """
    task_type = task_meta.get("task_type", "")
    validator = VALIDATORS.get(task_type)
    if validator is None:
        # No validator registered — accept executor's self-reported units_done
        return task_meta.get("units_done", 0)
    return validator.validate(output_path, task_meta)

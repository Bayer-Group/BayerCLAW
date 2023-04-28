import json
import logging

from ..src.runner.custom_logs import JSONFormatter
# from ..src.runner.version import VERSION


def test_JSONFormatter(monkeypatch):
    monkeypatch.setenv("BC_BRANCH_IDX", "99")
    monkeypatch.setenv("BC_EXECUTION_ID", "testExecutionId")
    monkeypatch.setenv("BC_LAUNCH_KEY", "testJobFile")
    monkeypatch.setenv("BC_LAUNCH_VERSION", "testVersion")
    monkeypatch.setenv("BC_LAUNCH_BUCKET", "testLaunchBucket")
    monkeypatch.setenv("BC_STEP_NAME", "testStepName")
    monkeypatch.setenv("BC_VERSION", "v1.2.3")
    monkeypatch.setenv("BC_WORKFLOW_NAME", "testWorkflowName")
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "0987654321")
    record = logging.LogRecord(
        name="test_name",
        pathname="test_pathname",
        func="test_func",
        lineno=999,
        args=(),
        exc_info=None,
        level=10,
        msg="test message")
    formatter = JSONFormatter()
    result_str = formatter.format(record)
    result = json.loads(result_str)
    expect = {
        "batch_job_id": "0987654321",
        "bclaw_version": f"v1.2.3",
        "branch": "99",
        "function": "test_pathname.test_func",
        "job_file": {
            "bucket": "testLaunchBucket",
            "key": "testJobFile",
            "version": "testVersion",
        },
        "level": logging.getLevelName(10),
        "message": "test message",
        "sfn_execution_id": "testExecutionId",
        "step_name": "testStepName",
        "workflow_name": "testWorkflowName",
    }
    assert result == expect

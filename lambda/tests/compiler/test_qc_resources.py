import pytest

from ...src.compiler.pkg.qc_resources import qc_checker_step, handle_qc_check
from ...src.compiler.pkg.util import Step, State


@pytest.mark.parametrize("next_step_name, next_or_end", [
    ("next_step", {"Next": "next_step"}),
    ("", {"End": True}),
])
def test_qc_checker_step(next_step_name, next_or_end, compiler_env):
    qc_spec = {
        "qc_result_file": "qc_file.json",
        "stop_early_if": "test_expression"
    }
    batch_step = Step("test_step", {"qc_check": qc_spec}, next_step_name)

    result = qc_checker_step(batch_step)
    expected = {
        "Type": "Task",
        "Resource": "qc_checker_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo.uri",
            "qc_result_file": "qc_file.json",
            "qc_expression": "test_expression",
            "execution_id.$": "$$.Execution.Id",
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "sfn_execution_id.$": "$$.Execution.Name",
                "step_name": "test_step",
                "workflow_name": "${WorkflowName}",
            },
        },
        "Retry": [
            {
                "ErrorEquals": ["QCFailed"],
                "IntervalSeconds": 3600,
                "MaxAttempts": 1,
            },
            {
                "ErrorEquals": ["States.ALL"],
                "IntervalSeconds": 3,
                "MaxAttempts": 3,
                "BackoffRate": 1.5
            }
        ],
        "ResultPath": None,
        "OutputPath": "$",
        **next_or_end
    }

    assert result == expected


def test_handle_qc_check(compiler_env):
    batch_spec = {
        "qc_check": {
            "qc_result_file": "qc_file.json",
            "stop_early_if": "test_expression",
            # email_subject and notification fields are no longer used, retaining them
            # here to test for backward compatibility
            "email_subject": "test subject",
            "notification": [
                "test1@case.com",
                "test2@case.com",
            ],
        },
    }

    batch_step = Step("step_name", batch_spec, "next_step")

    result = handle_qc_check(batch_step)

    expected_state_name = "step_name.qc_checker"

    assert isinstance(result, State)
    assert result.name == expected_state_name
    assert result.spec["Resource"] == "qc_checker_lambda_arn"
    assert result.spec["Parameters"]["qc_result_file"] == batch_spec["qc_check"]["qc_result_file"]
    assert result.spec["Parameters"]["qc_expression"] == batch_spec["qc_check"]["stop_early_if"]
    assert result.spec["Next"] == "next_step"

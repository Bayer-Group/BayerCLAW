import pytest

from ...src.compiler.pkg.qc_resources import qc_checker_step, handle_qc_check
from ...src.compiler.pkg.util import CoreStack, Step, State, SENTRY


@pytest.mark.parametrize("next_step, next_or_end", [(Step("next_step", {}), {"Next": "next_step"}),
                                                    (SENTRY, {"End": True})])
def test_qc_checker_step(next_step, next_or_end, monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    qc_spec = {
        "qc_result_file": "qc_file.json",
        "stop_early_if": "test_expression"
    }
    result = qc_checker_step(core_stack, "test_step", qc_spec, next_step)
    expected = {
        "Type": "Task",
        "Resource": "qc_checker_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo",
            "qc_result_file": "qc_file.json",
            "qc_expression": "test_expression",
            "execution_id.$": "$$.Execution.Id",
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "job_file_s3_request_id.$": "$.job_file.s3_request_id",
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


def test_handle_qc_check(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    qc_spec = {
        "qc_result_file": "qc_file.json",
        "stop_early_if": "test_expression",
        # email_subject and notification fields are no longer used, retaining them
        # here to test for backward compatibility
        "email_subject": "test subject",
        "notification": [
            "test1@case.com",
            "test2@case.com",
        ],
    }

    expected_step_name = "step_name.qc_checker"

    first_step_name, states = handle_qc_check(core_stack, "step_name", qc_spec, Step("next_step_name", {}))

    assert first_step_name == expected_step_name

    assert len(states) == 1
    assert isinstance(states[0], State)
    assert states[0].name == expected_step_name
    assert states[0].spec["Resource"] == "qc_checker_lambda_arn"
    assert states[0].spec["Parameters"]["qc_result_file"] == qc_spec["qc_result_file"]
    assert states[0].spec["Parameters"]["qc_expression"] == qc_spec["stop_early_if"]

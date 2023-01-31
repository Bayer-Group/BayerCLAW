import json
import textwrap

import pytest
import yaml

from ...src.compiler.pkg.subpipe_resources import file_submit_step, run_subpipe_step, file_retrieve_step, handle_subpipe
from ...src.compiler.pkg.util import Step, lambda_retry

SUBMIT_BLOCK = {
    "submit": [
        "file1.txt -> fileA.txt",
        "file2.txt",
    ],
}


@pytest.fixture(scope="module")
def sample_subpipe_spec() -> dict:
    ret = {
        "job_data": "test_job_data.json",
        **SUBMIT_BLOCK,
        "subpipe": "arn:aws:states:us-east-1:123456789012:StateMachine:test-machine",
        "retrieve": [
            "fileX.txt -> file3.txt",
            "fileY.txt",
        ],
    }
    return ret


def test_file_submit_step(sample_subpipe_spec, compiler_env):
    test_step = Step("step_name", sample_subpipe_spec, "next_step_name")
    result = file_submit_step(test_step, "run_subpipe_step_name")
    expect = {
        "Type": "Task",
        "Resource": "subpipes_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo",
            "job_data": "test_job_data.json",
            "submit": json.dumps(SUBMIT_BLOCK["submit"]),
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "sfn_execution_id.$": "$$.Execution.Name",
                "step_name": "step_name",
                "workflow_name": "${WorkflowName}",
            },
        },
        **lambda_retry(),
        "ResultPath": "$.subpipe",
        "OutputPath": "$",
        "Next": "run_subpipe_step_name",
    }
    assert result == expect


def test_run_subpipe_step(sample_subpipe_spec):
    test_step = Step("step_name", sample_subpipe_spec, "next_step_name")
    result = run_subpipe_step(test_step, "retrieve_step_name")
    expect = {
        "Type": "Task",
        "Resource": "arn:aws:states:::states:startExecution.sync",
        "Parameters": {
            "Input": {
                "index": "main",
                "job_file.$": "$.job_file",
                "prev_outputs": {},
                "repo.$": "$.subpipe.sub_repo",
                "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id",
            },
            "Name.$": "States.Format('{}_step_name', $$.Execution.Name)",
            "StateMachineArn": sample_subpipe_spec["subpipe"],
        },
        "ResultPath": None,
        "OutputPath": "$",
        "Next": "retrieve_step_name"
    }
    assert result == expect


@pytest.mark.parametrize("next_step_name, next_or_end", [
    ("next_step", {"Next": "next_step"}),
    ("", {"End": True}),
])
def test_file_retrieve_step(next_step_name, next_or_end, sample_subpipe_spec, compiler_env):
    test_step = Step("step_name", sample_subpipe_spec, next_step_name)
    result = file_retrieve_step(test_step)
    expect = {
        "Type": "Task",
        "Resource": "subpipes_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo",
            "retrieve": json.dumps(sample_subpipe_spec["retrieve"]),
            "subpipe": {
                "sub_repo.$": "$.subpipe.sub_repo",
            },
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "sfn_execution_id.$": "$$.Execution.Name",
                "step_name": "step_name",
                "workflow_name": "${WorkflowName}",
            },
        },
        **lambda_retry(),
        "ResultSelector": {},
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **next_or_end
    }
    assert result == expect


def test_handle_subpipe(sample_subpipe_spec, compiler_env):
    test_step = Step("step_name", sample_subpipe_spec, "next_step_name")
    states = handle_subpipe(test_step)
    assert len(states) == 3

    assert states[0].name == "step_name"
    assert states[0].spec["Next"] == "step_name.subpipe"

    assert states[1].name == "step_name.subpipe"
    assert states[1].spec["Next"] == "step_name.retrieve"

    assert states[2].name == "step_name.retrieve"
    assert states[2].spec["Next"] == "next_step_name"

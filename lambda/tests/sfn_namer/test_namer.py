import json
import re

import boto3
import moto
import pytest

from ...src.sfn_namer.namer import make_execution_name, lambda_handler


def test_make_execution_name():
    key = "one/two/three/four99+five____zireau88*six&^%$#.seven.eight"
    version = "1234567890ABCDEF1234567890ABCDEF"
    max_len = 40
    result = make_execution_name(key, version, max_len=max_len)
    assert re.match(r"^[A-Za-z0-9_-]{1,40}$", result)


@pytest.fixture(scope="function")
def mock_state_machine():
    with moto.mock_iam():
        iam = boto3.resource("iam", region_name="us-east-1")

        role = iam.create_role(
            RoleName="fakeRole",
            AssumeRolePolicyDocument="{}"
        )

        with moto.mock_stepfunctions():
            sfn = boto3.client("stepfunctions")
            state_machine = sfn.create_state_machine(
                name="test_sfn",
                definition="{}",
                roleArn=role.arn
            )

            yield state_machine["stateMachineArn"]


def test_lambda_handler(mock_state_machine):
    event = {
      "job_file": {
        "bucket": "bucket-name",
        "key": "wf-name/one/two/three/file.txt",
        "version": "0123456789ABCDEF0123456789abcdef"
      },
      "index": "main",
      "sfn_arn": mock_state_machine,
    }

    lambda_handler(event, {})

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=mock_state_machine)
    execution = result["executions"][0]
    assert execution["stateMachineArn"] == mock_state_machine
    assert execution["name"] == "one-two-three-file_01234567"
    assert execution["status"] == "RUNNING"

    desc = sfn.describe_execution(executionArn=execution["executionArn"])
    desc_input = json.loads(desc["input"])
    expect = {
        "job_file": {
            "bucket": "bucket-name",
            "key": "wf-name/one/two/three/file.txt",
            "version": "0123456789ABCDEF0123456789abcdef"
        },
        "index": "main",
    }
    assert desc_input == expect


def test_namer_duplicate_event(mock_state_machine):
    event1 = {
      "job_file": {
        "bucket": "bucket-name",
        "key": "wf-name/four/five/six/file.txt",
        "version": "abcdef0123456789ABCDEF0123456789"
      },
      "index": "main",
      "sfn_arn": mock_state_machine,
    }
    event2 = event1.copy()

    lambda_handler(event1, {})
    lambda_handler(event2, {})

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=mock_state_machine)
    assert len(result["executions"]) == 1

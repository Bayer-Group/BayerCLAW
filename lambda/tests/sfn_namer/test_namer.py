import json
import re

import boto3
import moto
import pytest

from ...src.sfn_namer.namer import normalize, make_execution_name, lambda_handler


def test_normalize():
    key = "DISCARD///two//three/four99+five____zireau88*six&^%$#.seven.DISCARD"
    expect = "two-three-four99-five-zireau88-six-seven"
    result = normalize(key)
    assert result == expect


def test_normalize_nothing_to_discard():
    key = "antidisestablishmentarianism"
    result = normalize(key)
    assert result == key


@pytest.mark.parametrize("replay", ["", "replay789ABCDEF"])
def test_make_execution_name_long_key(replay):
    key = f"dirname/{'a' * 90}"
    version = "12345678abcdefghijklmnopqrstuvwxyz"
    result = make_execution_name(key, version, replay)

    assert len(result) == 80
    assert re.match(r"^[A-Za-z0-9_-]{1,80}$", result)

    assert result.endswith(version[:8])
    if replay != "":
        assert result.startswith(replay[:10])


@pytest.mark.parametrize("replay", ["", "replay789ABCDEF"])
def test_make_execution_name_normalize(replay):
    key = "DISCARD/one!@#$%/TWO)(*&tHrEe<>?/456___789/file-name.DISCARD"
    exp_norm = "one-TWO-tHrEe-456-789-file-name"
    version = "12345678abcdefghijklmnopqrstuvwxyz"
    result = make_execution_name(key, version, replay)

    assert len(result) <= 80
    assert re.match(r"^[A-Za-z0-9_-]{1,80}$", result)

    assert result.endswith(version[:8])
    assert exp_norm in result
    if replay != "":
        assert result.startswith(replay[:10])


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


@pytest.mark.parametrize("replay, expected_name", [
    ("", "one-two-three-file_01234567"),
    ("replay789ABCDEF", "replay789A_one-two-three-file_01234567")
])
def test_lambda_handler(mock_state_machine, replay, expected_name):
    event = {
        "job_file": {
            "bucket": "bucket-name",
            "key": "wf-name/one/two/three/file.txt",
            "version": "0123456789ABCDEF0123456789abcdef"
        },
        "index": "main",
        "replay": replay,
        "sfn_arn": mock_state_machine,
    }

    lambda_handler(event, {})

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=mock_state_machine)
    execution = result["executions"][0]
    assert execution["stateMachineArn"] == mock_state_machine
    assert execution["name"] == expected_name
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


def test_lambda_handler_duplicate_event(mock_state_machine):
    event1 = {
        "job_file": {
            "bucket": "bucket-name",
            "key": "wf-name/four/five/six/file.txt",
            "version": "abcdef0123456789ABCDEF0123456789"
        },
        "index": "main",
        "replay": "",
        "sfn_arn": mock_state_machine,
    }
    event2 = event1.copy()

    lambda_handler(event1, {})
    lambda_handler(event2, {})

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=mock_state_machine)
    assert len(result["executions"]) == 1

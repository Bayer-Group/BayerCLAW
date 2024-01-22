import json
import re

import boto3
import moto
import pytest

from ...src.router.job_router import __name__ as router_name
from ...src.router.job_router import (get_state_machine_name, shorten_filename, normalize, make_execution_name,
                                      get_state_machine_arn, lambda_handler)


@pytest.mark.parametrize("key, expect", [
    ("NAME///two//three/four99+five____zireau88*six&^%$#.seven.eight", ("NAME", "two//three/four99+five____zireau88*six&^%$#.seven.eight"))
])
def test_get_state_machine_name(key, expect):
    result = get_state_machine_name(key)
    assert result == expect


@pytest.mark.parametrize("string, expect", [
    ("one///two//three/four99+five____zireau88*six&^%$#.seven.DISCARD", "one///two//three/four99+five____zireau88*six&^%$#.seven"),
    ("antidisestablishmentarianism", "antidisestablishmentarianism")
])
def test_shorten_filename(string, expect):
    result = shorten_filename(string)
    assert result == expect


def test_normalize():
    key = "two//three/four99+five____zireau88*six&^%$#.seven"
    expect = "two-three-four99-five-zireau88-six-seven"
    result = normalize(key)
    assert result == expect


def test_make_execution_name_normalize():
    key = "one!@#$%/TWO)(*&tHrEe<>?/456___789/file-name.DISCARD"
    exp_norm = "one-TWO-tHrEe-456-789-file-name"
    version = "12_34.5678_abcdefghijklmnopqrstuvwxyz"
    result = make_execution_name(key, version)

    assert len(result) <= 80
    assert re.match(r"^[A-Za-z0-9_-]{1,80}$", result)

    assert result.endswith("12-34-56")
    assert result.startswith(exp_norm)


def test_make_execution_name_long_key():
    key = f"dirname/{'a' * 90}"
    version = "12345678abcdefghijklmnopqrstuvwxyz"
    result = make_execution_name(key, version)

    assert len(result) == 80
    assert re.match(r"^[A-Za-z0-9_-]{1,80}$", result)

    assert result.endswith(version[:8])

@pytest.mark.parametrize("s3_key, version, expect", [
    (".leading_dot", "12345", "leading-dot_12345"),
    (".....dots.ext", "12345", "dots_12345"),
    ("_____.linz", "12345", "_12345"),
    ("unversioned", "", "unversioned_NULL"),
    ("base64.nightmare", "._._._._._", "base64_NULL"),
    ("_____.horrifying", "._._._._._", "_NULL"),
])
def test_make_execution_name_pathological(s3_key, version, expect):
    result = make_execution_name(s3_key, version)
    assert result == expect


def test_get_state_machine_arn(monkeypatch):
    monkeypatch.setenv("REGION", "us-west-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")

    result = get_state_machine_arn("test-state-machine")
    expect = "arn:aws:states:us-west-1:123456789012:stateMachine:test-state-machine:current"
    assert result == expect


@pytest.fixture(scope="function")
def mock_state_machine(monkeypatch):
    with moto.mock_iam():
        iam = boto3.resource("iam", region_name="us-west-1")

        role = iam.create_role(
            RoleName="fakeRole",
            AssumeRolePolicyDocument="{}"
        )

        with moto.mock_stepfunctions():
            sfn = boto3.client("stepfunctions", region_name="us-west-1")
            state_machine = sfn.create_state_machine(
                name="test_sfn",
                definition="{}",
                roleArn=role.arn
            )

            yield state_machine["stateMachineArn"]


def test_lambda_handler(mock_state_machine, monkeypatch, mocker):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-1")
    monkeypatch.setenv("REGION", "us-west-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")
    monkeypatch.setenv("SFN_NAME_ROOT", "test_sfn")
    monkeypatch.setenv("BC_VERSION", "v1.2.3")

    # as of 1/22/2024, moto's mock step functions service does not implement versions or aliases.
    # Therefore, I have to mock out get_state_machine_arn to give lambda_handler an unaliased arn.
    mock_state_machine_arn = mock_state_machine
    # mocker.patch("router.job_router.get_state_machine_arn", return_value=mock_state_machine_arn)
    mocker.patch(f"{router_name}.get_state_machine_arn", return_value=mock_state_machine_arn)

    event = {
        "branch": "main",
        "job_file_bucket": "bucket-name",
        "job_file_key": "wf-name/one/two/three/file.txt",
        "job_file_version": "0123456789ABCDEF0123456789abcdef",
    }

    lambda_handler(event, {})

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=mock_state_machine_arn)
    execution = result["executions"][0]
    assert execution["stateMachineArn"] == mock_state_machine_arn
    assert execution["name"] == "one-two-three-file_01234567"
    assert execution["status"] == "RUNNING"

    desc = sfn.describe_execution(executionArn=execution["executionArn"])
    desc_input = json.loads(desc["input"])
    expect = {
        "job_file": {
            "bucket": "bucket-name",
            "key": "wf-name/one/two/three/file.txt",
            "version": "0123456789ABCDEF0123456789abcdef",
        },
        "index": "main",
    }
    assert desc_input == expect


def test_main_duplicate_event(mock_state_machine, monkeypatch, mocker):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-1")
    monkeypatch.setenv("REGION", "us-west-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")
    monkeypatch.setenv("SFN_NAME_ROOT", "test_sfn")
    monkeypatch.setenv("BC_VERSION", "v1.2.3")

    mock_state_machine_arn = mock_state_machine
    mocker.patch(f"{router_name}.get_state_machine_arn", return_value=mock_state_machine_arn)

    event1 = {
        "branch": "main",
        "job_file_bucket": "bucket-name",
        "job_file_key": "wf-name/four/five/six/file.txt",
        "job_file_version": "abcdef0123456789ABCDEF0123456789",
    }
    event2 = event1.copy()

    lambda_handler(event1, {})
    lambda_handler(event2, {})

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=mock_state_machine_arn)
    assert len(result["executions"]) == 1

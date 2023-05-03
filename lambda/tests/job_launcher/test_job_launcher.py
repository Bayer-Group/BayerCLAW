import json
import logging
import re

import boto3
import moto
import pytest

from ...src.job_launcher.pkg.launcher_stuff import shorten_filename, normalize, make_execution_name, \
    make_state_machine_name, main

logging.basicConfig(level=logging.INFO)


@pytest.mark.parametrize("string, expect", [
    ("DISCARD///two//three/four99+five____zireau88*six&^%$#.seven.DISCARD", "two//three/four99+five____zireau88*six&^%$#.seven"),
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
    version = "12_34.5678_abcdefghijklmnopqrstuvwxyz"
    result = make_execution_name(key, version, replay)

    assert len(result) <= 80
    assert re.match(r"^[A-Za-z0-9_-]{1,80}$", result)

    assert result.endswith("12-34-56")
    assert exp_norm in result
    if replay != "":
        assert result.startswith(replay[:10])


@pytest.mark.parametrize("s3_key, version, expect", [
    ("bucket/.leading_dot", "12345", "leading-dot_12345"),
    ("bucket/.....dots.ext", "12345", "dots_12345"),
    ("bucket/_____.linz", "12345", "_12345"),
    ("bucket/unversioned", "", "unversioned_NULL"),
    ("bucket/base64.nightmare", "._._._._._", "base64_NULL"),
    ("bucket/_____.horrifying", "._._._._._", "_NULL"),
])
def test_make_execution_name_pathological(s3_key, version, expect):
    result = make_execution_name(s3_key, version, "")
    assert result == expect


@pytest.fixture(scope="function", params=["true", "false"])
def mock_state_machine_version(request):
    with moto.mock_iam():
        iam = boto3.resource("iam", region_name="us-east-1")

        role = iam.create_role(
            RoleName="fakeRole",
            AssumeRolePolicyDocument="{}"
        )

        with moto.mock_stepfunctions():
            sfn = boto3.client("stepfunctions")
            if request.param == "true":
                sfn_name = "test_sfn--99"
            else:
                sfn_name = "test_sfn"
            state_machine = sfn.create_state_machine(
                name=sfn_name,
                definition="{}",
                roleArn=role.arn
            )

            yield state_machine["stateMachineArn"], request.param


class MockContext():
    def __init__(self):
        self.function_version = 99


@pytest.mark.parametrize("replay, expected_name", [
    ("", "one-two-three-file_01234567"),
    ("replay789ABCDEF", "replay789A_one-two-three-file_01234567")
])
def test_main(mock_state_machine_version, replay, expected_name, monkeypatch):
    monkeypatch.setenv("REGION", "us-east-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")
    monkeypatch.setenv("SFN_NAME_ROOT", "test_sfn")
    monkeypatch.setenv("BC_VERSION", "v1.2.3")

    state_machine_arn, versioned = mock_state_machine_version
    monkeypatch.setenv("VERSIONED_SFN", versioned)

    event = {
        "branch": "main",
        "job_file_bucket": "bucket-name",
        "job_file_key": "wf-name/one/two/three/file.txt",
        "job_file_version": "0123456789ABCDEF0123456789abcdef",
        "replay": replay,
    }

    ctx = MockContext()

    main(event, ctx)

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=state_machine_arn)
    execution = result["executions"][0]
    assert execution["stateMachineArn"] == state_machine_arn
    assert execution["name"] == expected_name
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


@pytest.mark.parametrize("versioned, expect", [
    ("true", "test_sfn--99"),
    ("false", "test_sfn")
])
def test_make_state_machine_name(monkeypatch, versioned, expect):
    monkeypatch.setenv("SFN_NAME_ROOT", "test_sfn")
    monkeypatch.setenv("VERSIONED_SFN", versioned)
    ctx = MockContext()

    result = make_state_machine_name(ctx)
    assert result == expect


def test_main_duplicate_event(mock_state_machine_version, monkeypatch):
    monkeypatch.setenv("REGION", "us-east-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")
    monkeypatch.setenv("SFN_NAME_ROOT", "test_sfn")
    monkeypatch.setenv("BC_VERSION", "v1.2.3")

    state_machine_arn, versioned = mock_state_machine_version
    monkeypatch.setenv("VERSIONED_SFN", versioned)

    event1 = {
        "branch": "main",
        "job_file_bucket": "bucket-name",
        "job_file_key": "wf-name/four/five/six/file.txt",
        "job_file_version": "abcdef0123456789ABCDEF0123456789",
        "replay": "",
    }
    event2 = event1.copy()

    ctx = MockContext()

    main(event1, ctx)
    main(event2, ctx)

    sfn = boto3.client("stepfunctions")
    result = sfn.list_executions(stateMachineArn=state_machine_arn)
    assert len(result["executions"]) == 1

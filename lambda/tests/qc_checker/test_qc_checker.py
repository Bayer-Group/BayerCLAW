import logging

import boto3
import moto
import pytest

from ...src.qc_checker.qc_checker import lambda_handler, QCFailed

logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="function")
def mock_repo_bucket():
    with moto.mock_aws():
        bucket_name = "repo-bucket"
        s3 = boto3.resource("s3", region_name="us-east-1")
        bucket = s3.create_bucket(Bucket=bucket_name)
        yield bucket


@pytest.fixture(scope="function")
def mock_state_machine():
    with moto.mock_aws():
        iam = boto3.resource("iam", region_name="us-east-1")

        role = iam.create_role(
            RoleName="fakeRole",
            AssumeRolePolicyDocument="{}"
        )

        # with moto.mock_aws():
        sfn = boto3.client("stepfunctions", region_name="us-east-1")

        state_machine = sfn.create_state_machine(
            name="fakeStateMachine",
            definition="{}",
            roleArn=role.arn
        )

        yield state_machine["stateMachineArn"]


def test_lambda_handler(caplog, monkeypatch, mock_repo_bucket, mock_state_machine):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # start step functions execution, get execution id
    sfn = boto3.client("stepfunctions")
    sfn_execution = sfn.start_execution(
        stateMachineArn=mock_state_machine,
        name="kryzyzniak",
        input='{"in": "put"}',
    )

    # add qc file to repo bucket
    mock_repo_bucket.put_object(
        Body=b'{"x": 0}',
        Key="repo/qc_file.json"
    )

    event = {
        "repo": f"s3://{mock_repo_bucket.name}/repo",
        "qc_result_file": "qc_file.json",
        "qc_expression": "x >= 1",
        "execution_id": sfn_execution["executionArn"],
        "logging": {
            "job_file_key": "job/file",
            "step_name": "step.name",
        },
    }

    _ = lambda_handler(event, {})

    execution_desc = sfn.describe_execution(executionArn=sfn_execution["executionArn"])
    assert execution_desc["status"] == "RUNNING"
    assert "passed QC check" in caplog.text


def test_lambda_handler_fail(caplog, monkeypatch, mock_repo_bucket, mock_state_machine):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # start step functions execution, get execution id
    sfn = boto3.client("stepfunctions")
    sfn_execution = sfn.start_execution(
        stateMachineArn=mock_state_machine,
        name="kryzyzniak",
        input='{"in": "put"}',
    )

    # add qc file to repo bucket
    mock_repo_bucket.put_object(
        Body=b'{"x": 3}',
        Key="repo/qc_file.json"
    )

    event = {
        "repo": f"s3://{mock_repo_bucket.name}/repo",
        "qc_result_file": "qc_file.json",
        "qc_expression": "x >= 1",
        "execution_id": sfn_execution["executionArn"],
        "logging": {
            "job_file_key": "job/file",
            "step_name": "step.name",
        },
    }

    with pytest.raises(QCFailed, match=event["qc_expression"]):
        lambda_handler(event, {})

    execution_desc = sfn.describe_execution(executionArn=sfn_execution["executionArn"])

    # moto quirk: aborted mock stepfunctions executions end up in SUCCEEDED state
    assert execution_desc["status"] != "RUNNING"
    assert "failed QC check" in caplog.text

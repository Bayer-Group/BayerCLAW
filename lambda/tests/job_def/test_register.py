from copy import deepcopy
import json

import boto3
import jmespath
import moto
import pytest

from ...src.job_def.register import edit_spec, lambda_handler


@pytest.fixture(scope="function")
def job_def_spec() -> dict:
    ret = {
        "type": "container",
        "parameters": {
            "image": "mmm",
        },
        "containerProperties": {
            "image": "docker.io/library/ubuntu",
            "command": ["ls", "-l"],
            "jobRoleArn": "arn:aws:iam::123456789012:role/fake-role",
            "environment": [],
            "resourceRequirements": [
                {
                    "type": "VCPU",
                    "value": "1"
                },
                {
                    "type": "MEMORY",
                    "value": "1024"
                }
            ]
        },
        "schedulingPriority": 1,
        "propagateTags": True,
        "tags": {
            "bclaw:workflow": "",
        }
    }
    return ret


@pytest.fixture(scope="function")
def event_factory(job_def_spec):
    def _ret(request_type: str, physical_resource_id: str):
        ret = {
            "RequestType": request_type,
            "RequestId": "fake-request_id",
            "ResponseURL": "https://fake.response.url",
            "ResourceType": "Custom::FakeCustomResourceType",
            "LogicalResourceId": "fake_resource_name",
            "StackId": "arn:aws:cloudformation:us-west-1:namespace:stack/stack-name/uuid",
            "PhysicalResourceId": physical_resource_id,
            "ResourceProperties": {
                "workflowName": "test-wf",
                "stepName": "test-step",
                "image": "docker.io/library/ubuntu",
                "spec": json.dumps(job_def_spec, sort_keys=True),
            }
        }
        return ret
    return _ret


class FakeContext:
    def __init__(self):
        self.log_group_name = "fake-log-group"
        self.log_stream_name = "fake-log-stream"


@pytest.fixture(scope="function")
def batch_job_def_arn(job_def_spec, monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-1")
    with moto.mock_aws():
        batch = boto3.client("batch")
        yld = batch.register_job_definition(jobDefinitionName="test-wf_test-step", **job_def_spec)
        yield yld["jobDefinitionArn"]


def test_edit_spec(job_def_spec, monkeypatch):
    monkeypatch.setenv("REGION", "us-west-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")

    test_image_spec = {
        "name": "test_image",
        "auth": "test_auth",
    }

    expect = deepcopy(job_def_spec) | {"jobDefinitionName": "test-wf_test-step"}
    expect["containerProperties"]["environment"] += [{"name": "BC_WORKFLOW_NAME", "value": "test-wf"},
                                                     {"name": "BC_STEP_NAME", "value": "test-step"},
                                                     {"name": "AWS_DEFAULT_REGION", "value": "us-west-1"},
                                                     {"name": "AWS_ACCOUNT_ID", "value": "123456789012"},]
    expect["parameters"]["image"] = json.dumps(test_image_spec,sort_keys=True, separators=(",", ":"))
    expect["tags"]["bclaw:workflow"] = "test-wf"
    result = edit_spec(job_def_spec, "test-wf", "test-step", test_image_spec)
    assert result == expect


@moto.mock_aws()
def test_lambda_handler_create(event_factory, mocker, monkeypatch):
    monkeypatch.setenv("REGION", "us-west-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-1")

    mock_respond_fn = mocker.patch("lambda.src.job_def.register.respond")
    event = event_factory("Create", "yadaYada")

    _ = lambda_handler(event, FakeContext())

    expected_job_def_name = "test-wf_test-step"
    expected_job_def_arn = f"arn:aws:batch:us-west-1:123456789012:job-definition/{expected_job_def_name}:1"
    expected_respond_call = {
        "PhysicalResourceId": expected_job_def_arn,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Status": "SUCCESS",
        "Reason": "",
        "NoEcho": False,
        "Data": {
            "Arn": expected_job_def_arn,
        },
    }

    mock_respond_fn.assert_called_once_with(event["ResponseURL"], expected_respond_call)

    batch = boto3.client("batch")
    response = batch.describe_job_definitions(jobDefinitionName=expected_job_def_name)
    job_defs = response["jobDefinitions"]
    assert len(job_defs) == 1
    assert job_defs[0]["jobDefinitionArn"] == expected_job_def_arn
    assert job_defs[0]["status"] == "ACTIVE"
    assert job_defs[0]["containerProperties"]["environment"] == [{"name": "BC_WORKFLOW_NAME", "value": "test-wf"},
                                                                 {"name": "BC_STEP_NAME", "value": "test-step"},
                                                                 {"name": "AWS_DEFAULT_REGION", "value": "us-west-1"},
                                                                 {"name": "AWS_ACCOUNT_ID", "value": "123456789012"},]
    assert job_defs[0]["tags"]["bclaw:workflow"] == "test-wf"


def test_lambda_handler_update(event_factory, batch_job_def_arn, mocker, monkeypatch):
    monkeypatch.setenv("REGION", "us-west-1")
    monkeypatch.setenv("ACCT_NUM", "123456789012")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-1")

    mock_respond_fn = mocker.patch("lambda.src.job_def.register.respond")
    event = event_factory("Update", batch_job_def_arn)

    _ = lambda_handler(event, FakeContext())

    expected_job_def_name = "test-wf_test-step"
    expected_job_def_arn = f"arn:aws:batch:us-west-1:123456789012:job-definition/{expected_job_def_name}:2"
    expected_respond_call = {
        "PhysicalResourceId": expected_job_def_arn,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Status": "SUCCESS",
        "Reason": "",
        "NoEcho": False,
        "Data": {
            "Arn": expected_job_def_arn
        }
    }

    mock_respond_fn.assert_called_once_with(event["ResponseURL"], expected_respond_call)

    batch = boto3.client("batch")
    response = batch.describe_job_definitions(jobDefinitionName=expected_job_def_name)
    job_defs = response["jobDefinitions"]
    assert len(job_defs) == 2
    arns = jmespath.search("[].jobDefinitionArn", job_defs)
    assert set(arns) == {batch_job_def_arn, expected_job_def_arn}
    statuses = jmespath.search("[].status", job_defs)
    assert all(s == "ACTIVE" for s in statuses)


def test_lambda_handler_delete(event_factory, batch_job_def_arn, mocker):
    mock_respond_fn = mocker.patch("lambda.src.job_def.register.respond")
    event = event_factory("Delete", batch_job_def_arn)

    _ = lambda_handler(event, FakeContext())

    expect = {
        "PhysicalResourceId": batch_job_def_arn,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Status": "SUCCESS",
        "Reason": "",
        "NoEcho": False,
        "Data": {}
    }

    mock_respond_fn.assert_called_once_with(event["ResponseURL"], expect)

    batch = boto3.client("batch")
    response = batch.describe_job_definitions(jobDefinitionName="test-wf_test-step")
    job_defs = response["jobDefinitions"]
    # job definition is still there, it just gets deactivated
    assert len(job_defs) == 1
    assert job_defs[0]["jobDefinitionArn"] == batch_job_def_arn
    assert job_defs[0]["status"] == "INACTIVE"


def test_lambda_handler_no_physical_resource_id(event_factory, batch_job_def_arn, mocker):
    mock_respond_fn = mocker.patch("lambda.src.job_def.register.respond")
    event = event_factory("Delete", "wut")
    event.pop("PhysicalResourceId")

    _ = lambda_handler(event, FakeContext())

    expect = {
        "PhysicalResourceId": None,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Status": "SUCCESS",
        "Reason": "",
        "NoEcho": False,
        "Data": {}
    }

    mock_respond_fn.assert_called_once_with(event["ResponseURL"], expect)


@moto.mock_aws()
def test_lambda_handler_fail(event_factory, mocker):
    mock_respond_fn = mocker.patch("lambda.src.job_def.register.respond")
    event = event_factory("Create", "wut")
    event.pop("ResourceProperties")
    ctx = FakeContext()

    _ = lambda_handler(event, FakeContext())

    expect = {
        "PhysicalResourceId": "wut",
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Status": "FAILED",
        "Reason": f"see log group {ctx.log_group_name} / log stream {ctx.log_stream_name}",
        "NoEcho": False,
        "Data": {}
    }

    mock_respond_fn.assert_called_once_with(event["ResponseURL"], expect)

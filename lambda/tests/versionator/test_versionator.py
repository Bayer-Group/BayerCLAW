from textwrap import dedent

import boto3
import moto
import pytest

from ...src.versionator.versionator import lambda_handler
import cfn_responder


class FakeContext:
    def __init__(self):
        self.log_stream_name = "fake-log-stream"


@pytest.fixture(scope="module")
def test_role():
    with moto.mock_iam():
        client = boto3.client("iam")
        result = client.create_role(
            RoleName="test-role",
            AssumeRolePolicyDocument=dedent("""/
                "Version": "2012-10-17",
                "Statement": {
                  "Effect": "Allow",
                  "Principal": { "Service": "lambda.amazonaws.com" }
                  "Action": "sts:AssumeRole"
                }
            """)
        )
        yield result


@pytest.fixture(scope="module")
def test_lambda(test_role):
    with moto.mock_lambda():
        client = boto3.client("lambda")
        response = client.create_function(
            FunctionName="test-function",
            Role=test_role["Role"]["Arn"],
            Code={
                "ZipFile": b""
            },
            Handler="fake.handler",
            Runtime="python3.9",
        )
        yield response


@pytest.mark.parametrize("req_type, version", [
    ("Create", "1"),
    ("Update", "2"),
    ("Delete", "don't care")])
def test_lambda_handler(req_type, version, test_lambda, mocker):
    mocker.patch("cfn_responder.respond")
    event = {
        "RequestType": req_type,
        "RequestId": "fake-request-id",
        "ResponseURL": "https://fake.response.url",
        "ResourceType": "Custom::FakeCustomResourceType",
        "LogicalResourceId": "fake_resource_name",
        "StackId": "arn:aws:cloudformation:us-east-2:namespace:stack/stack-name/uuid",
        "ResourceProperties": {
            "FunctionName": "test-function",
        },
    }
    ctx = FakeContext()

    expect = {
        "PhysicalResourceId": ctx.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Status": "SUCCESS",
        "Reason": "",
        "NoEcho": False,
    }

    if req_type == "Delete":
        expect["Data"] = dict()
    else:
        expect["Data"] = {
            "Arn": test_lambda["FunctionArn"] + ":" + version,
            "Version": version,
        }

    lambda_handler(event, ctx)
    cfn_responder.respond.assert_called_once_with(event["ResponseURL"], expect)


def test_lambda_handler_fail(mocker):
    mocker.patch("cfn_responder.respond")
    event = {
        "RequestType": "Create",
        "RequestId": "fake-request-id",
        "ResponseURL": "https://fake.response.url",
        "ResourceType": "Custom::FakeCustomResourceType",
        "LogicalResourceId": "fake_resource_name",
        "StackId": "arn:aws:cloudformation:us-east-2:namespace:stack/stack-name/uuid",
        "ResourceProperties": {
            "FunctionName": "not-a-function",
        },
    }
    ctx = FakeContext()

    expect = {
        "PhysicalResourceId": ctx.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Status": "FAILED",
        "Reason": f"see log stream {ctx.log_stream_name}",
        "NoEcho": False,
        "Data": {},
    }

    lambda_handler(event, ctx)
    cfn_responder.respond.assert_called_once_with(event["ResponseURL"], expect)

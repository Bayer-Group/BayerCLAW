import json
import os
import sys

import boto3
from moto import mock_sns
import pytest
import yaml

# make common layer modules available
sys.path.append(
    os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),  # (home)/lambda/tests/scatter
            os.pardir,                  # (home)/lambda/tests
            os.pardir,                  # (home)/lambda
            "src", "common", "python"
        )
    )
)

from ...src.notifications.notifications import make_sfn_console_url, make_state_change_message, \
    make_message_attributes, make_sns_payload, lambda_handler
from ...src.notifications.notifications import AWS_SFN_CONSOLE_URL_BASE as URL_BASE

WORKFLOW_NAME = "test_workflow"

REGION = "us-east-1"
EXECUTION_NAME = "12345678-etc-etc"
EXECUTION_ARN = f"arn:aws:states:{REGION}:123456789012:execution:test:{EXECUTION_NAME}"

LAUNCHER_BUCKET = "test-bucket"
JOB_DATA_KEY = "path/to/job.json"
JOB_DATA_VERSION = "1234567890"
JOB_DATA_URI = f"s3://{LAUNCHER_BUCKET}/{JOB_DATA_KEY}"
REQUEST_ID = "ELVISLIVES"


@pytest.fixture(scope="module")
def state_change_event_factory():
    input_obj = {
        "job_file": {
            "bucket": LAUNCHER_BUCKET,
            "key": JOB_DATA_KEY,
            "version": JOB_DATA_VERSION,
            "s3_request_id": REQUEST_ID,
        },
        "index": "main",
    }

    def _event_impl(topic: str = "fake_topic", status: str = "UNKNOWN") -> dict:
        ret = {
            "workflow_name": WORKFLOW_NAME,
            "sns_topic_arn": topic,
            "event": {
                "detailType": "Step Functions Execution Status Change",
                "region": REGION,
            },
            "detail": {
                "executionArn": EXECUTION_ARN,
                "name": EXECUTION_NAME,
                "status": status,
                "input": json.dumps(input_obj),
                "inputDetails": {
                    "included": True
                },
            },
        }
        return ret

    return _event_impl


def test_make_sfn_console_url():
    result = make_sfn_console_url(region=REGION, exec_arn=EXECUTION_ARN)
    expect = f"{URL_BASE}?region={REGION}#/executions/details/{EXECUTION_ARN}"
    assert result == expect


@pytest.mark.parametrize("status, action", [
    ("RUNNING", "has started."),
    ("SUCCEEDED", "has finished."),
    ("FAILED", "has failed."),
    ("ABORTED", "has been aborted."),
    ("TIMED_OUT", "has timed out."),
])
def test_make_state_change_message(state_change_event_factory, status, action):
    event = state_change_event_factory(status=status)

    expected_details = {
        "details": {
            "workflow_name": WORKFLOW_NAME,
            "sfn_execution_id": EXECUTION_NAME,
            "job_status": status,
            "job_data": JOB_DATA_URI,
            "s3_request_id": REQUEST_ID,
            "sfn_console_link": f"{URL_BASE}?region={REGION}#/executions/details/{EXECUTION_ARN}",
        },
    }

    expected_job_handle = EXECUTION_NAME.split("-", 1)[0]

    message = make_state_change_message(event)
    text, details = yaml.safe_load_all(message)

    assert WORKFLOW_NAME in text
    assert "job.json" in text
    assert expected_job_handle in text
    assert text.endswith(action)

    assert details == expected_details


def test_make_message_attributes(state_change_event_factory):
    event = state_change_event_factory(status="FAKE_STATUS")
    result = make_message_attributes(event)
    expect = {
        "status": {
            "DataType": "String",
            "StringValue": "FAKE_STATUS",
        },
        "workflow_name": {
            "DataType": "String",
            "StringValue": WORKFLOW_NAME,
        },
        "execution_id": {
            "DataType": "String",
            "StringValue": EXECUTION_NAME,
        },
        "launcher_bucket": {
            "DataType": "String",
            "StringValue": LAUNCHER_BUCKET,
        },
        "job_file": {
            "DataType": "String",
            "StringValue": JOB_DATA_KEY,
        },
        "job_file_version": {
            "DataType": "String",
            "StringValue": JOB_DATA_VERSION,
        },
        "s3_request_id": {
            "DataType": "String",
            "StringValue": REQUEST_ID,
        },
    }
    assert result == expect


def test_make_sns_publish_payload(state_change_event_factory):
    event = state_change_event_factory(status="SUCCEEDED", topic="arn:of:fake:topic")
    result = make_sns_payload("test message", event)
    expect = {
        "TopicArn": "arn:of:fake:topic",
        "Message": "test message",
        "Subject": f"{WORKFLOW_NAME}: job succeeded",
        "MessageAttributes": make_message_attributes(event),
    }
    assert result == expect


@mock_sns
def test_lambda_handler(monkeypatch, state_change_event_factory):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    sns = boto3.client("sns")
    response0 = sns.create_topic(Name="test_topic")

    event = state_change_event_factory(status="SUCCEEDED", topic=response0["TopicArn"])

    response = lambda_handler(event, {})
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

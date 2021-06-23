from datetime import datetime, timedelta
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2
import pytest

from ...src.job_status.job_status import lambda_handler


@pytest.fixture(scope="function")
def ddb_table():
    with mock_dynamodb2():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        yld = dynamodb.create_table(
            AttributeDefinitions=[
                {
                    "AttributeName": "workflowName",
                    "AttributeType": "S",
                },
                {
                    "AttributeName": "executionId",
                    "AttributeType": "S",
                },
            ],
            TableName="testTable",
            KeySchema=[
                {
                    "AttributeName": "workflowName",
                    "KeyType": "HASH",
                },
                {
                    "AttributeName": "executionId",
                    "KeyType": "RANGE",
                }
            ],
            BillingMode="PAY_PER_REQUEST"
        )

        yield yld


@pytest.mark.parametrize("status", ["RUNNING", "SUCCEEDED", "FAILED", "ABORTED"])
def test_lambda_handler(status, ddb_table, monkeypatch):
    monkeypatch.setenv("JOB_STATUS_TABLE", "testTable")
    monkeypatch.setenv("EXPIRATION_DAYS", "90")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    timestamp_str = "2021-05-28T17:53:54.991Z"
    timestamp_obj = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    expiration_obj = timestamp_obj + timedelta(days=90)
    expected_timestamp = Decimal(int(timestamp_obj.timestamp()))
    expected_expiration = Decimal(int(expiration_obj.timestamp()))

    event = {
        "Records": [
            {
                "Sns": {
                    "Timestamp": timestamp_str,
                    "MessageAttributes": {
                        "execution_id": {
                            "Value": "12345678-1234...",
                        },
                        "workflow_name": {
                            "Value": "test-workflow",
                        },
                        "job_file": {
                            "Value": "test-workflow/path/to/job.file",
                        },
                        "job_file_version": {
                            "Value": "987654321",
                        },
                        "status": {
                            "Value": status,
                        }
                    }
                }
            }
        ]
    }

    lambda_handler(event, {})

    chek = ddb_table.query(
        KeyConditionExpression=Key("workflow_name").eq("test-workflow"),
        Select="ALL_ATTRIBUTES"
    )

    expect = {
        "executionId": "12345678-1234...",
        "workflowName": "test-workflow",
        "jobFile": "path/to/job.file#987654321",
        # "jobFileName": "path/to/job.file",
        # "jobFileVersion": "987654321",
        "status": status,
        "timestamp": expected_timestamp,
        "expiration": expected_expiration,
    }
    assert chek["Items"][0] == expect


@pytest.mark.parametrize("old_status, new_status, expected_file_version", [
    ("RUNNING", "RUNNING", "new"),
    ("SUCCEEDED", "RUNNING", "old"),
    ("FAILED", "RUNNING", "old"),
    ("ABORTED", "RUNNING", "old"),
    ("RUNNING", "SUCCEEDED", "new"),
    ("RUNNING", "FAILED", "new"),
    ("RUNNING", "ABORTED", "new"),
])
def test_lambda_handler_overwrite(old_status, new_status, expected_file_version, ddb_table, monkeypatch):
    monkeypatch.setenv("JOB_STATUS_TABLE", "testTable")
    monkeypatch.setenv("EXPIRATION_DAYS", "90")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    ddb_table.put_item(
        Item={
            "workflowName": "test-workflow",
            "executionId": "12345678-1234...",
            "status": old_status,
            "jobFile": "path/to/job.file#old"
            # "jobFile": "path/to/job.file",
            # "jobFileVersion": "old"
        }
    )

    event = {
        "Records": [
            {
                "Sns": {
                    "Timestamp": "2021-05-28T17:53:54.991Z",
                    "MessageAttributes": {
                        "execution_id": {
                            "Value": "12345678-1234...",
                        },
                        "workflow_name": {
                            "Value": "test-workflow",
                        },
                        "job_file": {
                            "Value": "test-workflow/path/to/job.file",
                        },
                        "job_file_version": {
                            "Value": "new",
                        },
                        "status": {
                            "Value": new_status,
                        },
                    }
                }
            }
        ]
    }

    lambda_handler(event, {})

    chek = ddb_table.query(
        KeyConditionExpression=Key("workflow_name").eq("test-workflow"),
        Select="ALL_ATTRIBUTES"
    )

    # assert chek["Items"][0]["jobFileVersion"] == expected_file_version
    assert chek["Items"][0]["jobFile"].endswith(expected_file_version)
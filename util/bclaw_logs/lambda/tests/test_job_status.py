import datetime as dt
from decimal import Decimal
import json

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2
import pytest

from ..src.job_status import lambda_handler


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

    timestamp_str = "2023-02-24T15:18:13Z"

    event = {
        "time": timestamp_str,
        "detail": {
            "name": "12345678-1234...",
            "status": status,
            "input": json.dumps(
                {
                    "job_file": {
                        "key": "test-workflow/path/to/job.file",
                        "version": "987654321",
                    },
                }
            ),
        },
    }

    lambda_handler(event, {})

    chek = ddb_table.query(
        KeyConditionExpression=Key("workflow_name").eq("test-workflow"),
        Select="ALL_ATTRIBUTES"
    )

    timestamp_obj = dt.datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
    expiration_obj = timestamp_obj + dt.timedelta(days=90)
    expected_timestamp = Decimal(int(timestamp_obj.timestamp()))
    expected_expiration = Decimal(int(expiration_obj.timestamp()))

    expect = {
        "executionId": "12345678-1234...",
        "workflowName": "test-workflow",
        "jobFile": "path/to/job.file#987654321",
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
        }
    )

    event = {
        "time": "2021-05-28T17:53:54Z",
        "detail": {
            "name": "12345678-1234...",
            "status": new_status,
            "input": json.dumps(
                {
                    "job_file": {
                        "key": "test-workflow/path/to/job.file",
                        # job file versions won't actually change during a run, this is just
                        # a hack to check whether the record was overwritten
                        "version": "new",
                    },
                }
            ),
        },
    }

    lambda_handler(event, {})

    chek = ddb_table.query(
        KeyConditionExpression=Key("workflow_name").eq("test-workflow"),
        Select="ALL_ATTRIBUTES"
    )

    assert chek["Items"][0]["jobFile"].endswith("#" + expected_file_version)

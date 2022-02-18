import json

import boto3
import yaml

AWS_SFN_CONSOLE_URL_BASE = "https://console.aws.amazon.com/states/home"


def make_sfn_console_url(region: str, exec_arn: str) -> str:
    ret = f"{AWS_SFN_CONSOLE_URL_BASE}?region={region}#/executions/details/{exec_arn}"
    return ret


def make_state_change_message(event: dict) -> str:
    workflow_name = event["workflow_name"]
    execution_id = event["detail"]["name"]
    status = event["detail"]["status"]

    input_obj = json.loads(event["detail"]["input"])

    console_url = make_sfn_console_url(region=event["event"]["region"],
                                       exec_arn=event["detail"]["executionArn"])

    details = {
        "details": {
            "workflow_name": workflow_name,
            "sfn_execution_id": execution_id,
            "job_status": status,
            "job_data": f"s3://{input_obj['job_file']['bucket']}/{input_obj['job_file']['key']}",
            "s3_request_id": input_obj["job_file"]["s3_request_id"],
            "sfn_console_link": console_url,
        },
    }

    if status == "RUNNING":
        action = "has started"

    elif status == "SUCCEEDED":
        action = "has finished"

    elif status == "FAILED":
        action = "has failed"

    elif status == "ABORTED":
        action = "has been aborted"

    elif status == "TIMED_OUT":
        action = "has timed out"

    else:
        raise RuntimeError(f"status {status} not recognized")

    execution_handle = execution_id.split("-", 1)[0]
    job_file_name = input_obj["job_file"]["key"].rsplit("/", 1)[-1]

    text = f"Job {execution_handle} ('{job_file_name}') on workflow {workflow_name} {action}."
    message = yaml.safe_dump_all([text, details])

    return message


def make_message_attributes(event: dict) -> dict:
    input_obj = json.loads(event["detail"]["input"])

    ret = {
        "status": {
            "DataType": "String",
            "StringValue": event["detail"]["status"],
        },
        "workflow_name": {
            "DataType": "String",
            "StringValue": event["workflow_name"],
        },
        "execution_id": {
            "DataType": "String",
            "StringValue": event["detail"]["name"],
        },
        "launcher_bucket": {
            "DataType": "String",
            "StringValue": input_obj["job_file"]["bucket"],
        },
        "job_file": {
            "DataType": "String",
            "StringValue": input_obj["job_file"]["key"],
        },
        "job_file_version": {
            "DataType": "String",
            "StringValue": input_obj["job_file"]["version"],
        },
        "s3_request_id": {
            "DataType": "String",
            "StringValue": input_obj["job_file"]["s3_request_id"],
        },
    }

    return ret


def make_sns_payload(message: str, event: dict) -> dict:
    ret = {
        "TopicArn": event["sns_topic_arn"],
        "Message": message,
        "Subject": f"{event['workflow_name']}: job {event['detail']['status'].lower()}",
        "MessageAttributes": make_message_attributes(event),
    }
    return ret


def submit_timestream_record(event: dict) -> None:
    status = event["detail"]["status"]

    if status == "RUNNING":
        version = 1
    elif status == "SUCCEEDED":
        version = 3
    else:
        version = 2

    input_obj = json.loads(event["detail"]["input"])

    records = [
        {
            "Dimensions": [
                {
                    "Name": "workflow_name",
                    "Value": event["workflow_name"],
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "job_file",
                    "Value": input_obj["job_file"]["key"],
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "job_file_version",
                    "Value": input_obj["job_file"]["version"],
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "execution_arn",
                    "Value": event["detail"]["executionArn"],
                    "DimensionValueType": "VARCHAR",
                },
            ],
            "MeasureName": "status",
            "MeasureValue": status,
            "MeasureValueType": "VARCHAR",
            "Time": str(event["detail"]["startDate"]),
            "TimeUnit": "MILLISECONDS",
            "Version": version,
        },
    ]

    ts_client = boto3.client("timestream-write")
    response = ts_client.write_records(DatabaseName="bclawTest",
                                       TableName="bclawTable",
                                       Records=records)


def lambda_handler(event: dict, context: object) -> dict:
    print(str(event))

    submit_timestream_record(event)

    message = make_state_change_message(event)
    payload = make_sns_payload(message, event)

    client = boto3.client("sns")
    response = client.publish(**payload)

    return response

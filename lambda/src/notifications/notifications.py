import json
import logging
import os

import boto3
import yaml

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def make_state_change_message(attributes: dict) -> str:
    status = attributes["status"]["StringValue"]
    workflow_name = attributes["workflow_name"]["StringValue"]
    execution_id = attributes["execution_id"]["StringValue"]
    job_file_bucket = attributes["job_file_bucket"]["StringValue"]
    job_file_key = attributes["job_file_key"]["StringValue"]
    job_file_version = attributes["job_file_version"]["StringValue"]

    details = {
        "details": {
            "workflow_name": workflow_name,
            "execution_id": execution_id,
            "job_status": status,
            "job_data": f"s3://{job_file_bucket}/{job_file_key}",
            "job_data_version": job_file_version,
        }
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

    job_file_name = job_file_key.rsplit("/", 1)[-1]

    text = f"Job {execution_id} ('{job_file_name}') on workflow {workflow_name} {action}."
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
            "StringValue": event["detail"]["stateMachineArn"].rsplit(":", 1)[-1],
        },
        "execution_id": {
            "DataType": "String",
            "StringValue": event["detail"]["name"],
        },
        "job_file_bucket": {
            "DataType": "String",
            "StringValue": input_obj["job_file"]["bucket"],
        },
        "job_file_key": {
            "DataType": "String",
            "StringValue": input_obj["job_file"]["key"],
        },
        "job_file_version": {
            "DataType": "String",
            "StringValue": input_obj["job_file"]["version"],
        },
    }

    return ret


def make_sns_payload(message: str, attributes: dict) -> dict:
    status = attributes["status"]["StringValue"]
    workflow_name = attributes["workflow_name"]["StringValue"]

    ret = {
        "TopicArn": os.environ["TOPIC_ARN"],
        "Message": message,
        "Subject": f"{workflow_name}: job {status.lower()}",
        "MessageAttributes": attributes,
    }
    return ret


def lambda_handler(event: dict, context: object) -> dict:
    print(f"{event=}")

    try:
        attributes = make_message_attributes(event)
        message = make_state_change_message(attributes)
        payload = make_sns_payload(message, attributes)

        client = boto3.client("sns")
        response = client.publish(**payload)

        return response

    except KeyError:
        logger.warning("unable to parse BayerCLAW information from event")

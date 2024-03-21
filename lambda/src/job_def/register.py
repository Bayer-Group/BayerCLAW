from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
import http.client
import json
import logging
import os
from typing import Generator
import urllib.parse

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass()
class Response:
    PhysicalResourceId: str
    StackId: str
    RequestId: str
    LogicalResourceId: str
    Status: str = "FAILED"
    Reason: str = ""
    NoEcho: bool = False
    Data: dict = field(default_factory=dict)

    def return_this(self, **kwargs):
        self.Data.update(**kwargs)


def respond(url: str, body: dict):
    url_obj = urllib.parse.urlparse(url)
    body_json = json.dumps(body)

    https = http.client.HTTPSConnection(url_obj.hostname)
    https.request("PUT", url_obj.path + "?" + url_obj.query, body_json)


@contextmanager
def responder(event, context, no_echo=False) -> Generator[Response, None, None]:
    response = Response(
        PhysicalResourceId=event.get("PhysicalResourceId", context.log_stream_name),
        StackId=event["StackId"],
        RequestId=event["RequestId"],
        LogicalResourceId=event["LogicalResourceId"],
        NoEcho=no_echo
    )
    try:
        yield response
        logger.info("succeeded")
        response.Status = "SUCCESS"
    except:
        logger.exception("failed: ")
        response.Reason = f"see log stream {context.log_stream_name}"
    finally:
        logger.info(f"{asdict(response)=}")
        respond(event["ResponseURL"], asdict(response))


def edit_spec(spec: dict, wf_name: str, step_name: str, image: str) -> dict:
    ret = spec.copy()
    ret["jobDefinitionName"] = f"{wf_name}_{step_name}"
    ret["containerProperties"]["environment"] += [{"name": "BC_WORKFLOW_NAME", "value": wf_name},
                                                  {"name": "BC_STEP_NAME", "value": step_name},
                                                  {"name": "AWS_DEFAULT_REGION", "value": os.environ["REGION"]},
                                                  {"name": "AWS_ACCOUNT_ID", "value": os.environ["ACCT_NUM"]}]
    ret["parameters"]["image"] = image
    return ret


def lambda_handler(event: dict, context: object):
    # event[ResourceProperties] = {
    #   workflowName: str
    #   stepName: str
    #   image: str
    #   spec: "{
    #     type: str
    #     parameters: {str: str}
    #     containerProperties: {
    #       image: str
    #       command: [str]
    #       jobRoleArn: str
    #       volumes: [dict]
    #       environment: [{name: str, value: str}]
    #       mountPoints: [dict]
    #       resourceRequirements: [{value: str, type: str}]
    #     }
    #     schedulingPriority: int
    #     tags: dict
    #   }"
    # }

    logger.info(f"{event=}")

    batch = boto3.client("batch")

    with responder(event, context) as cfn_response:
        if event["RequestType"] in ["Create", "Update"]:
            spec0 = json.loads(event["ResourceProperties"]["spec"])
            spec = edit_spec(spec0,
                             event["ResourceProperties"]["workflowName"],
                             event["ResourceProperties"]["stepName"],
                             event["ResourceProperties"]["image"])
            logger.info(f"{spec=}")

            result = batch.register_job_definition(**spec)
            cfn_response.PhysicalResourceId = result["jobDefinitionArn"]
            cfn_response.return_this(Arn=result["jobDefinitionArn"])

        else:
            try:
                batch.deregister_job_definition(jobDefinition=event["PhysicalResourceId"])
            except:
                logger.exception("deregistration failed: ")

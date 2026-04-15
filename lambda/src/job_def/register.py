"""
Prior to v1.2.8, this was the business end of Lambda-backed custom CloudFormation resources (having
logical names ending with JobDefx). The custom resource is no longer needed due to updates in the way
CloudFormation handles Batch job definitions. However, this Lambda still needs to be present so that
CloudFormation can call it when updating old workflows to v1.2.8. It doesn't do anything (other than
complain when asked to create or update a resource), but it still must return a well-formed response
when called to delete a resource.

Old docstring:
When CloudFormation updates a Batch job definition, it will deactivate the old version automatically. This doesn't
work well with blue/green deployments, where we want to keep the old version active in case a rollback is required.
This lambda function will register a new version of the job definition without deactivating the old one. It is meant
to be used as a custom resource in CloudFormation.
"""

from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
import http.client
import json
import logging
from typing import Generator
import urllib.parse

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
        PhysicalResourceId=event.get("PhysicalResourceId"),
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
        response.Reason = f"see log group {context.log_group_name} / log stream {context.log_stream_name}"
    finally:
        logger.info(f"{asdict(response)=}")
        respond(event["ResponseURL"], asdict(response))


def lambda_handler(event: dict, context: object):
    # event[ResourceProperties] = {
    #   workflowName: str
    #   stepName: str
    #   image: dict  # str
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
    #     consumableResourceProperties: dict
    #     schedulingPriority: int
    #     timeout: dict
    #     propagateTags: bool
    #     tags: dict
    #   }"
    # }

    logger.info(f"{event=}")

    with responder(event, context):
        if event["RequestType"] in ["Create", "Update"]:
            raise RuntimeError(f"don't call me")

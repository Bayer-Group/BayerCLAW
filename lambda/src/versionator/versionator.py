from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
import http.client
import json
import urllib.parse

import boto3

# note: target function needs to have been changed
#   ...environment var change is sufficient

# remove cfn_responder from common layer
# make VersionatorLambda "immutable"
#   remove common layer
#   remove AutoPublishAlias
#   remove BCLAW_VERSION environment var
# replace VERSIONATOR_LAMBDA_ARN in compiler

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

    def return_values(self, **kwargs):
        self.Data.update(**kwargs)


def respond(url: str, body: dict):
    url_obj = urllib.parse.urlparse(url)
    body_json = json.dumps(body)

    https = http.client.HTTPSConnection(url_obj.hostname)
    https.request("PUT", url_obj.path + "?" + url_obj.query, body_json)


@contextmanager
def responder(event, context, no_echo=False):
    response = Response(
        PhysicalResourceId=context.log_stream_name,
        StackId=event["StackId"],
        RequestId=event["RequestId"],
        LogicalResourceId=event["LogicalResourceId"],
        NoEcho=no_echo
    )
    try:
        yield response
        response.Status = "SUCCESS"
    except:
        response.Reason = f"see log stream {context.log_stream_name}"
    finally:
        respond(event["ResponseURL"], asdict(response))


def lambda_handler(event: dict, context: object):
    with responder(event, context) as cfn_response:
        if event["RequestType"] in ["Create", "Update"]:
            function_name = event["ResourceProperties"]["FunctionName"]

            client = boto3.client("lambda")

            result = client.publish_version(FunctionName=function_name)
            cfn_response.return_values(Arn=result["FunctionArn"],
                                       Version=result["Version"])

        else:
            # no-op for Delete requests
            pass

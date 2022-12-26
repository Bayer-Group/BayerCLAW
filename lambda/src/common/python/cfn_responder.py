from contextlib import contextmanager
from dataclasses import dataclass, asdict, field
import http.client
import json
import urllib.parse

# todo: test

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
        raise
    finally:
        url = urllib.parse.urlparse(event["ResponseURL"])
        body = json.dumps(asdict(response))

        https = http.client.HTTPSConnection(url.hostname)
        https.request("PUT", url.path + "?" + url.query, body)

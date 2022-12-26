import boto3

from cfn_responder import responder

# note: target function needs to have been changed
#   ...environment var change is sufficient


def lambda_handler(event: dict, context: object):
    with responder(event, context) as cfn_response:
        if event["RequestType"] in ["Create", "Update"]:
            function_name = event["ResourceProperties"]["FunctionName"]

            client = boto3.client("lambda")

            # todo: description??? might help with manual cleanup
            result = client.publish_version(FunctionName=function_name)
            cfn_response.return_values(Arn=result["FunctionArn"],
                                       Version=result["Version"])

        else:
            # no-op for Delete requests
            pass

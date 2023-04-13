import os
import uuid

from .util import Resource

DEPLOY_STACK_NAME = "deployStack"
LAUNCHER_STACK_NAME = "launcherStack"


def launcher_substack_rc(options: dict) -> Resource:
    rc_bucket = os.environ["RESOURCE_BUCKET_NAME"]
    source_version = os.environ["SOURCE_VERSION"]
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/{source_version}/wf_launcher.yaml"
    # versioned_sfn = "" if options["versioned"] else "N"

    ret = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "LauncherImageUri": os.environ["JOB_LAUNCHER_REPO_URI"] + ":" + os.environ["SOURCE_VERSION"],
                "LogRetentionDays": os.environ["LOG_RETENTION_DAYS"],
                "LoggingDestinationArn": os.environ["LOGGING_DESTINATION_ARN"],
                "Uniqifier": str(uuid.uuid4()),
                "VersionatorArn": os.environ["VERSIONATOR_LAMBDA_ARN"],
                "VersionedSFN": options["versioned"],
                "WorkflowName": {"Ref": "AWS::StackName"},
            },
            "TemplateURL": template_url,
        }
    }

    return Resource(LAUNCHER_STACK_NAME, ret)


def deploy_substack_rc(state_machine_logical_name: str) -> Resource:
    rc_bucket = os.environ["RESOURCE_BUCKET_NAME"]
    source_version = os.environ["SOURCE_VERSION"]
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/{source_version}/wf_deploy.yaml"

    ret = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "LauncherBucketName": os.environ["LAUNCHER_BUCKET_NAME"],
                "LauncherLambdaName": {
                    "Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaName"],
                },
                "LauncherLambdaVersion": {
                    "Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaVersion"],
                },
                "NotificationsLambdaArn": os.environ["NOTIFICATIONS_LAMBDA_ARN"],
                "StateMachineArn": {"Ref": state_machine_logical_name},
                "WorkflowName": {"Ref": "AWS::StackName"},
            },
            "TemplateURL": template_url,
        },
    }

    return Resource(DEPLOY_STACK_NAME, ret)

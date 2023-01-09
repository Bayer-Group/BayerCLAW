import os
import uuid

from .util import CoreStack, Resource

DEPLOY_STACK_NAME = "deployStack"
LAUNCHER_STACK_NAME = "launcherStack"


def launcher_substack_rc(core_stack: CoreStack) -> Resource:
    rc_bucket = core_stack.output("ResourceBucketName")
    source_version = os.environ["SOURCE_VERSION"]
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/{source_version}/wf_launcher.yaml"

    ret = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "LauncherImageUri": core_stack.output("JobLauncherImageUri"),
                "LogRetentionDays": core_stack.output("LogRetentionDays"),
                "Uniqifier": str(uuid.uuid4()),
                "VersionatorArn": core_stack.output("VersionatorLambdaArn"),
                "WorkflowName": {"Ref": "AWS::StackName"},
            },
            "TemplateURL": template_url,
        }
    }

    return Resource(LAUNCHER_STACK_NAME, ret)


def deploy_substack_rc(core_stack: CoreStack, state_machine_logical_name: str) -> Resource:
    rc_bucket = core_stack.output("ResourceBucketName")
    source_version = os.environ["SOURCE_VERSION"]
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/{source_version}/wf_deploy.yaml"

    ret = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "LauncherBucketName": core_stack.output("LauncherBucketName"),
                "LauncherLambdaName": {
                    "Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaName"],
                },
                "LauncherLambdaVersion": {
                    "Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaVersion"],
                },
                "NotificationsLambdaArn": core_stack.output("EventHandlerLambdaArn"),
                "StateMachineArn": {"Ref": state_machine_logical_name},
                "WorkflowName": {"Ref": "AWS::StackName"},
            },
            "TemplateURL": template_url,
        },
    }

    return Resource(DEPLOY_STACK_NAME, ret)

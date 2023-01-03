import os

from .util import CoreStack, Resource

DEPLOY_STACK_NAME = "deployStack"
LAUNCHER_STACK_NAME = "launcherStack"
# NOTIFICATIONS_STACK_NAME = "notificationsStack"


def launcher_substack_rc(core_stack: CoreStack, state_machine_logical_name: str) -> Resource:
    rc_bucket = core_stack.output("ResourceBucketName")
    # build_id = core_stack.output("TODO")  # todo
    # build_id = os.environ.get("SOURCE_VERSION")
    # template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/wf_launcher.yaml"
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/wf_launcher2.yaml"

    ret = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "WorkflowName": {"Ref": "AWS::StackName"},
                "StateMachineArn": {"Ref": state_machine_logical_name},
                "LauncherBucketName": core_stack.output("LauncherBucketName"),
                "NamerLambdaArn": core_stack.output("NamerLambdaArn"),
            },
            "TemplateURL": template_url,
        }
    }

    return Resource(LAUNCHER_STACK_NAME, ret)


def deploy_substack_rc(core_stack: CoreStack, state_machine_logical_name: str) -> Resource:
    rc_bucket = core_stack.output("ResourceBucketName")
    # build_id = core_stack.output("TODO")  # todo
    # build_id = os.environ.get("SOURCE_VERSION")
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/wf_deploy.yaml"

    ret = {
        "Type": "AWS::Cloudformation::Stack",
        "Properties": {
            "Parameters": {
                "LauncherBucketName": core_stack.output("LauncherBucketName"),
                "LauncherLambdaName": {
                    "Fn::GetAtt", [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaName"],
                },
                "LauncherLambdaVersion": {
                    "Fn::GetAtt", [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaVersion"],
                },
                "NotificationsLambdaArn": core_stack.output("EventHandlerLambdaArn"),
                "StateMachineArn": {"Ref": state_machine_logical_name},
                "WorkflowName": {"Ref": "AWS::StackName"},
            }
        },
        "TemplateURL": template_url,
    }

    return Resource(DEPLOY_STACK_NAME, ret)


# todo: remove
# def notifications_substack_rc(core_stack: CoreStack, state_machine_logical_name: str) -> Resource:
#     rc_bucket = core_stack.output("ResourceBucketName")
#     template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/wf_notifications.yaml"
#     handler_lambda_arn = core_stack.output("EventHandlerLambdaArn")
#     job_status_lambda_arn = core_stack.output("JobStatusLambdaArn")
#
#     ret = {
#         "Type": "AWS::CloudFormation::Stack",
#         "Properties": {
#             "Parameters": {
#                 "WorkflowName": {"Ref": "AWS::StackName"},
#                 "HandlerLambdaArn": handler_lambda_arn,
#                 "JobStatusLambdaArn": job_status_lambda_arn,
#                 "StateMachineArn": {"Ref": state_machine_logical_name},
#             },
#             "TemplateURL": template_url,
#         },
#     }
#
#     return Resource(NOTIFICATIONS_STACK_NAME, ret)

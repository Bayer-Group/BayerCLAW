from .util import CoreStack, Resource

LAUNCHER_STACK_NAME = "launcherStack"
NOTIFICATIONS_STACK_NAME = "notificationsStack"


def launcher_substack_rc(core_stack: CoreStack, state_machine_logical_name: str) -> Resource:
    rc_bucket = core_stack.output("ResourceBucketName")
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/wf_launcher.yaml"

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


def notifications_substack_rc(core_stack: CoreStack, state_machine_logical_name: str) -> Resource:
    rc_bucket = core_stack.output("ResourceBucketName")
    template_url = f"https://s3.amazonaws.com/{rc_bucket}/cloudformation/wf_notifications.yaml"
    handler_lambda_arn = core_stack.output("EventHandlerLambdaArn")
    job_status_lambda_arn = core_stack.output("JobStatusLambdaArn")

    ret = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "WorkflowName": {"Ref": "AWS::StackName"},
                "HandlerLambdaArn": handler_lambda_arn,
                "JobStatusLambdaArn": job_status_lambda_arn,
                "StateMachineArn": {"Ref": state_machine_logical_name},
            },
            "TemplateURL": template_url,
        },
    }

    return Resource(NOTIFICATIONS_STACK_NAME, ret)

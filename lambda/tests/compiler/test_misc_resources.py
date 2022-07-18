from ...src.compiler.pkg.misc_resources import launcher_substack_rc, notifications_substack_rc, \
    LAUNCHER_STACK_NAME, NOTIFICATIONS_STACK_NAME
from ...src.compiler.pkg.util import CoreStack, Resource


def test_launcher_substack_rc(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()
    notifs = Resource("fake_notifs", {})

    result = launcher_substack_rc(core_stack, "FakeStateMachine", notifs)
    expect = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "WorkflowName": {"Ref": "AWS::StackName"},
                "StateMachineArn": {"Ref": "FakeStateMachine"},
                "LauncherBucketName": "launcher_bucket_name",
                "NamerLambdaArn": "namer_lambda_arn",
                "NotificationsTopicArn": {"Fn::GetAtt": [notifs.name, "Outputs.wfOutputTopicArn"]}
            },
            "TemplateURL": "https://s3.amazonaws.com/resource_bucket_name/cloudformation/wf_launcher.yaml",
        },
    }
    assert isinstance(result, Resource)
    assert result.name == LAUNCHER_STACK_NAME
    assert result.spec == expect


def test_notifications_substack_rc(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()
    
    result = notifications_substack_rc(core_stack, "FakeStateMachine")
    expect = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "WorkflowName": {"Ref": "AWS::StackName"},
                "HandlerLambdaArn": "event_handler_lambda_arn",
                "JobStatusLambdaArn": "job_status_lambda_arn",
                "StateMachineArn": {"Ref": "FakeStateMachine"},
            },
            "TemplateURL": "https://s3.amazonaws.com/resource_bucket_name/cloudformation/wf_notifications.yaml",
        },
    }
    assert isinstance(result, Resource)
    assert result.name == NOTIFICATIONS_STACK_NAME
    assert result.spec == expect

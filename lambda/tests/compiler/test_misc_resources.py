from ...src.compiler.pkg.misc_resources import launcher_substack_rc, deploy_substack_rc, \
    LAUNCHER_STACK_NAME, DEPLOY_STACK_NAME
from ...src.compiler.pkg.misc_resources import uuid as uu
from ...src.compiler.pkg.util import Resource


def test_launcher_substack_rc(monkeypatch, compiler_env):
    monkeypatch.setattr(uu, "uuid4", lambda: "fake_uuid")
    options = {"versioned": "Y"}

    result = launcher_substack_rc(options)
    expect = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "LauncherImageUri": "job_launcher_repo_uri:1234567",
                "LogRetentionDays": "99",
                "Uniqifier": "fake_uuid",
                "VersionatorArn": "versionator_lambda_arn",
                "VersionedSFN": "Y",
                "WorkflowName": {"Ref": "AWS::StackName"},
            },
            "TemplateURL": "https://s3.amazonaws.com/resource_bucket_name/cloudformation/1234567/wf_launcher.yaml",
        },
    }
    assert isinstance(result, Resource)
    assert result.name == LAUNCHER_STACK_NAME
    assert result.spec == expect


def test_deploy_substack_rc(monkeypatch, compiler_env):
    state_machine_logical_name = "FakeStateMachineLogicalName"

    result = deploy_substack_rc(state_machine_logical_name)
    expect = {
        "Type": "AWS::CloudFormation::Stack",
        "Properties": {
            "Parameters": {
                "LauncherBucketName": "launcher_bucket_name",
                "LauncherLambdaName": {
                    "Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaName"],
                },
                "LauncherLambdaVersion": {
                    "Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaVersion"],
                },
                "NotificationsLambdaArn": "notifications_lambda_arn",
                "StateMachineArn": {"Ref": state_machine_logical_name},
                "WorkflowName": {"Ref": "AWS::StackName"},
            },
            "TemplateURL": "https://s3.amazonaws.com/resource_bucket_name/cloudformation/1234567/wf_deploy.yaml"
        },
    }

    assert isinstance(result, Resource)
    assert result.name == DEPLOY_STACK_NAME
    assert result.spec == expect

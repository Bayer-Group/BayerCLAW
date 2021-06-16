import os
import textwrap

import boto3
import moto
import pytest


@pytest.fixture(scope="session")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test-access-key-id"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test-secret-access-key"
    os.environ["AWS_SECURITY_TOKEN"] = "test-security-token"
    os.environ["AWS_SESSION_TOKEN"] = "test-session-token"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="session")
def mock_core_stack(aws_credentials):
    ami_gen = moto.ec2.ec2_backend.describe_images()
    ami = next(iter(ami_gen))

    template = textwrap.dedent(f"""
      AWSTemplateFormatVersion: 2010-09-09
      Resources:
        Instance:
          Type: AWS::EC2::Instance
          Properties:
            ImageId: {ami.id}

      Outputs:
        ChooserLambdaArn:
          Value: chooser_lambda_arn
        ECSTaskRoleArn:
          Value: ecs_task_role_arn
        EFSVolumeId:
          Value: efs_volume_id
        EventHandlerLambdaArn:
          Value: event_handler_lambda_arn
        GatherLambdaArn:
          Value: gather_lambda_arn
        LauncherBucketName:
          Value: launcher_bucket_name
        LauncherLambdaArn:
          Value: launcher_lambda_arn
        LogRetentionDays:
          Value: 99
        OnDemandQueueArn:
          Value: on_demand_queue_arn
        QCCheckerLambdaArn:
          Value: qc_checker_lambda_arn
        ResourceBucketName:
          Value: resource_bucket_name
        ScatterLambdaArn:
          Value: scatter_lambda_arn
        SpotQueueArn:
          Value: spot_queue_arn
        StatesExecutionRoleArn:
          Value: states_execution_role_arn
        SubpipesLambdaArn:
          Value: subpipes_lambda_arn
    """)

    with moto.mock_cloudformation():
        cfn = boto3.client("cloudformation")
        cfn.create_stack(
            StackName="bclaw-core",
            TemplateBody=template
        )
        yield boto3.resource("cloudformation").Stack("bclaw-core")

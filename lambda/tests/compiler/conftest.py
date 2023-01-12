import os
import textwrap

import boto3
import moto
import moto.ec2
import pytest


@pytest.fixture(scope="session")
def compiler_env():
    os.environ["CORE_STACK_NAME"] = "bclaw-core"
    os.environ["CHOOSER_LAMBDA_ARN"] = "chooser_lambda_arn"
    os.environ["ECS_TASK_ROLE_ARN"] = "ecs_task_role_arn"
    os.environ["EVENT_HANDLER_LAMBDA_ARN"] = "event_handler_lambda_arn"
    os.environ["GATHER_LAMBDA_ARN"] = "gather_lambda_arn"
    os.environ["ON_DEMAND_QUEUE_ARN"] = "on_demand_queue_arn"
    os.environ["INITIALIZER_LAMBDA_ARN"] = "initializer_lambda_arn"
    os.environ["JOB_LAUNCHER_REPO_URI"] = "job_launcher_repo_uri"
    os.environ["LAUNCHER_BUCKET_NAME"] = "launcher_bucket_name"
    os.environ["LOG_RETENTION_DAYS"] = "99"
    os.environ["QC_CHECKER_LAMBDA_ARN"] = "qc_checker_lambda_arn"
    os.environ["RESOURCE_BUCKET_NAME"] = "resource_bucket_name"
    os.environ["RUNNER_REPO_URI"] = "runner_repo_uri"
    os.environ["SCATTER_LAMBDA_ARN"] = "scatter_lambda_arn"
    os.environ["SOURCE_VERSION"] = "1234567"
    os.environ["SPOT_QUEUE_ARN"] = "spot_queue_arn"
    os.environ["STATES_EXECUTION_ROLE_ARN"] = "states_execution_role_arn"
    os.environ["SUBPIPES_LAMBDA_ARN"] = "subpipes_lambda_arn"
    os.environ["VERSIONATOR_LAMBDA_ARN"] = "versionator_lambda_arn"

# todo: remove?
@pytest.fixture(scope="module")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test-access-key-id"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test-secret-access-key"
    os.environ["AWS_SECURITY_TOKEN"] = "test-security-token"
    os.environ["AWS_SESSION_TOKEN"] = "test-session-token"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


# todo: remove?
@pytest.fixture(scope="module")
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
        EventHandlerLambdaArn:
          Value: event_handler_lambda_arn
        GatherLambdaArn:
          Value: gather_lambda_arn
        InitializerLambdaArn:
          Value: initializer_lambda_arn
        JobLauncherImageUri:
          Value: job_launcher_image_uri
        xxxJobStatusLambdaArn:
          Value: job_status_lambda_arn
        LauncherBucketName:
          Value: launcher_bucket_name
        LogRetentionDays:
          Value: "99"
        xOnDemandQueueArn:
          Value: on_demand_queue_arn
        QCCheckerLambdaArn:
          Value: qc_checker_lambda_arn
        ResourceBucketName:
          Value: resource_bucket_name
        RunnerImageUri:
          Value: runner_image_uri
        ScatterLambdaArn:
          Value: scatter_lambda_arn
        xSpotQueueArn:
          Value: spot_queue_arn
        StatesExecutionRoleArn:
          Value: states_execution_role_arn
        SubpipesLambdaArn:
          Value: subpipes_lambda_arn
        VersionatorLambdaArn:
          Value: versionator_lambda_arn
    """)

    with moto.mock_cloudformation():
        cfn = boto3.client("cloudformation")
        cfn.create_stack(
            StackName="bclaw-core",
            TemplateBody=template
        )
        yield boto3.resource("cloudformation").Stack("bclaw-core")

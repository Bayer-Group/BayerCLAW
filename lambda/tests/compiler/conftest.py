import os

import pytest


@pytest.fixture(scope="session")
def compiler_env():
    os.environ["CENTRAL_LOGGING_DESTINATION_ARN"] = "central_logging_destination_arn"
    os.environ["CORE_STACK_NAME"] = "bclaw-core"
    os.environ["CHOOSER_LAMBDA_ARN"] = "chooser_lambda_arn"
    os.environ["ECS_TASK_ROLE_ARN"] = "ecs_task_role_arn"
    os.environ["NOTIFICATIONS_LAMBDA_ARN"] = "notifications_lambda_arn"
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

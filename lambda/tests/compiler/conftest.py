import os

import pytest


@pytest.fixture(scope="session")
def compiler_env():
    os.environ.update({
        "CORE_STACK_NAME": "bclaw-core",
        "CHOOSER_LAMBDA_ARN": "chooser_lambda_arn",
        "ECS_TASK_ROLE_ARN": "ecs_task_role_arn",
        "NOTIFICATIONS_LAMBDA_ARN": "notifications_lambda_arn",
        "GATHER_LAMBDA_ARN": "gather_lambda_arn",
        "ON_DEMAND_QUEUE_ARN": "on_demand_queue_arn",
        "INITIALIZER_LAMBDA_ARN": "initializer_lambda_arn",
        "JOB_LAUNCHER_REPO_URI": "job_launcher_repo_uri",
        "LAUNCHER_BUCKET_NAME": "launcher_bucket_name",
        "LOG_RETENTION_DAYS": "99",
        "LOGGING_DESTINATION_ARN": "logging_destination_arn",
        "QC_CHECKER_LAMBDA_ARN": "qc_checker_lambda_arn",
        "RESOURCE_BUCKET_NAME": "resource_bucket_name",
        "RUNNER_REPO_URI": "runner_repo_uri",
        "SCATTER_INIT_LAMBDA_ARN": "scatter_init_lambda_arn",
        "SCATTER_LAMBDA_ARN": "scatter_lambda_arn",
        "SOURCE_VERSION": "1234567",
        "SPOT_QUEUE_ARN": "spot_queue_arn",
        "STATES_EXECUTION_ROLE_ARN": "states_execution_role_arn",
        "SUBPIPES_LAMBDA_ARN": "subpipes_lambda_arn",
        "VERSIONATOR_LAMBDA_ARN": "versionator_lambda_arn",
    })

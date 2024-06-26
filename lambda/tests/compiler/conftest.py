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
        "ON_DEMAND_GPU_QUEUE_ARN": "on_demand_gpu_queue_arn",
        "ON_DEMAND_QUEUE_ARN": "on_demand_queue_arn",
        "INITIALIZER_LAMBDA_ARN": "initializer_lambda_arn",
        "JOB_DEF_LAMBDA_ARN": "job_def_lambda_arn",
        "LAUNCHER_BUCKET_NAME": "launcher_bucket_name",
        "LOG_RETENTION_DAYS": "99",
        "LOGGING_DESTINATION_ARN": "logging_destination_arn",
        "RESOURCE_BUCKET_NAME": "resource_bucket_name",
        "RUNNER_REPO_URI": "runner_repo_uri",
        "SCATTER_INIT_LAMBDA_ARN": "scatter_init_lambda_arn",
        "SCATTER_LAMBDA_ARN": "scatter_lambda_arn",
        "SOURCE_VERSION": "1234567",
        "SPOT_GPU_QUEUE_ARN": "spot_gpu_queue_arn",
        "SPOT_QUEUE_ARN": "spot_queue_arn",
        "STATES_EXECUTION_ROLE_ARN": "states_execution_role_arn",
        "SUBPIPES_LAMBDA_ARN": "subpipes_lambda_arn",
    })

import logging
import os

logger = logging.getLogger(__name__)


def log_preamble():
    logger.info(f"workflow_name={os.environ['BC_WORKFLOW_NAME']}")
    logger.info(f"step_name={os.environ['BC_STEP_NAME']}")
    logger.info(f"job_file=s3://{os.environ['BC_LAUNCH_BUCKET']}/{os.environ['BC_LAUNCH_KEY']}:{os.environ['BC_LAUNCH_VERSION']}")
    logger.info(f"sfn_execution_id={os.environ['BC_EXECUTION_ID']}")
    logger.info(f"branch={os.environ['BC_BRANCH_IDX']}")
    logger.info(f"batch_job_id={os.environ['AWS_BATCH_JOB_ID']}")
    logger.info(f"bclaw_version={os.environ['BC_VERSION']}")

from contextlib import contextmanager
import json
import logging
import os
from textwrap import dedent


def log_preamble(logger: logging.Logger,
                 branch: str = "N/A",
                 job_file_bucket: str = "N/A",
                 job_file_key: str = "N/A",
                 job_file_version: str = "N/A",
                 sfn_execution_id: str = "N/A",
                 step_name: str = "N/A",
                 workflow_name: str = "N/A") -> None:
    logger.info(dedent(f"""---------- preamble ----------
        {workflow_name=}
        {step_name=}
        job_file=s3://{job_file_bucket}/{job_file_key}:{job_file_version}
        {sfn_execution_id=}
        {branch=}
        bclaw_version={os.environ.get("BCLAW_VERSION", "N/A")}
    """))


def log_event(logger: logging.Logger, event: dict) -> None:
    logger.info("---------- event ----------" + json.dumps(event, indent=2))

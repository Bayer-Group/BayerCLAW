from contextlib import contextmanager
import json
import logging
import os


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        obj = {
            "level": record.levelname,
            "message": record.getMessage(),
            "function": f"{record.module}.{record.funcName}",
        }

        if hasattr(record, "custom"):
            obj.update(record.custom)

        if record.exc_info is not None:
            obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(obj)


@contextmanager
def custom_lambda_logs(branch: str = "unknown",
                       job_file_bucket: str = "unknown",
                       job_file_key: str = "unknown",
                       job_file_version: str = "unknown",
                       sfn_execution_id: str = "unknown",
                       step_name: str = "unknown",
                       workflow_name: str = "unknown",
                       **kwargs) -> None:
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)

        record.custom = {
            "workflow_name": workflow_name,
            "step_name": step_name,
            "job_file": {
                "bucket": job_file_bucket,
                "key": job_file_key,
                "version": job_file_version,
            },
            "sfn_execution_id": sfn_execution_id,
            "branch": branch,
            "bclaw_version": os.environ.get("BCLAW_VERSION", "unknown")
        }

        return record

    try:
        logging.setLogRecordFactory(record_factory)
        yield

    finally:
        logging.setLogRecordFactory(old_factory)


def log_preamble(logger: logging.Logger,
                 branch: str = "N/A",
                 job_file_bucket: str = "N/A",
                 job_file_key: str = "N/A",
                 job_file_version: str = "N/A",
                 sfn_execution_id: str = "N/A",
                 step_name: str = "N/A",
                 workflow_name: str = "N/A") -> None:
    logger.info(f"{workflow_name=}")
    logger.info(f"{step_name=}")
    logger.info(f"job_file=s3://{job_file_bucket}/{job_file_key}:{job_file_version}")
    logger.info(f"{sfn_execution_id=}")
    logger.info(f"{branch=}")
    logger.info(f"bclaw_version={os.environ['BC_VERSION']}")

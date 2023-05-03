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
def custom_lambda_logs(bclaw_version: str = "unknown",
                       branch: str = "unknown",
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
            "bclaw_version": bclaw_version + "woohoo"
        }

        return record

    try:
        logging.setLogRecordFactory(record_factory)
        yield

    finally:
        logging.setLogRecordFactory(old_factory)

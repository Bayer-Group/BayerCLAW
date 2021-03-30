from contextlib import contextmanager
import json
import logging


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
                       job_file_s3_request_id: str = "unknown",
                       sfn_execution_id: str = "unknown",
                       step_name: str = "unknown",
                       workflow_name: str = "unknown") -> None:
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)

        record.custom = {
            "branch": branch,
            "job_file": {
                "bucket": job_file_bucket,
                "key": job_file_key,
                "version": job_file_version,
                "s3_request_id": job_file_s3_request_id,
            },
            "step_name": step_name,
            "sfn_execution_id": sfn_execution_id,
            "workflow_name": workflow_name,
        }

        return record

    try:
        logging.setLogRecordFactory(record_factory)
        yield

    finally:
        logging.setLogRecordFactory(old_factory)

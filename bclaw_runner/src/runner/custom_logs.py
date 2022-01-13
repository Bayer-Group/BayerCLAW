import json
import logging
import os

from .version import VERSION


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        obj = {
            "level": record.levelname,
            "message": record.getMessage(),
            "function": f"{record.module}.{record.funcName}",
            "workflow_name": os.environ.get("BC_WORKFLOW_NAME"),
            "step_name": os.environ.get("BC_STEP_NAME"),
            "job_file": {
                "bucket": os.environ.get("BC_LAUNCH_BUCKET"),
                "key": os.environ.get("BC_LAUNCH_KEY"),
                "version": os.environ.get("BC_LAUNCH_VERSION"),
                "s3_request_id": os.environ.get("BC_LAUNCH_S3_REQUEST_ID"),
            },
            "sfn_execution_id": os.environ.get("BC_EXECUTION_ID"),
            "branch": os.environ.get("BC_BRANCH_IDX"),
            "batch": {
                "runner": f"bclaw_runner v{VERSION}",
                "job_id": os.environ.get("AWS_BATCH_JOB_ID"),
            },
        }
        if record.exc_info is not None:
            obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(obj)


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "()": JSONFormatter
        },
    },
    "handlers": {
        "cloudwatch": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "json",
        },
    },
    "loggers": {
        "": {
            "handlers": ["cloudwatch"],
            "level": "INFO"
        }
    }
}

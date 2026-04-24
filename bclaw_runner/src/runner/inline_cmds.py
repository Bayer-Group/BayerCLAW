import logging
import os
import re
from typing import Generator

import boto3

logger = logging.getLogger(__name__)


class UserDefinedError(Exception):
    def __init__(self, title: str, message: str):
        super().__init__(message)
        self.title = title


def abort(text: str, _) -> None:
    logger.warning(f"aborting workflow execution with message {text}")

    region = os.environ["AWS_DEFAULT_REGION"]
    acct = os.environ["AWS_ACCOUNT_ID"]
    wf_name = os.environ["BC_WORKFLOW_NAME"]
    exec_id = os.environ["BC_EXECUTION_ID"]
    step_name = os.environ["BC_STEP_NAME"]
    execution_arn = f"arn:aws:states:{region}:{acct}:execution:{wf_name}:{exec_id}"

    sfn = boto3.client("stepfunctions")
    sfn.stop_execution(
        executionArn=execution_arn,
        error=f"Job {exec_id} aborted at step {step_name}",
        cause=text
    )


def error(title: str, message: str) -> None:
    logger.warning(f"raising user defined error with title {title} and message {message}")
    raise UserDefinedError(title, message)


def opt_parser(opts: str) -> Generator[tuple[str, str], None, None]:
    for kv in re.split(r"\s+", opts):
        k, v = kv.split(r"=", maxsplit=1)
        yield k, v


# ::command opt1=val1 opt2=val2::string
CMD_PARSER = re.compile("^::([a-z]+)(?:\s+(.+))*::(.*)$")

FN_MAP = {
    "abort": abort,
    "error": error,
}

def parse_for_commands(line: str) -> None:
    if m := CMD_PARSER.match(line):
        cmd, opt_str, text = m.groups()
        logger.info(f"{cmd=}")
        logger.info(f"{opt_str=}")
        logger.info(f"{text=}")
        try:
            opts = dict(opt_parser(opt_str))
        except TypeError:
            opts = {}

        try:
            fn = FN_MAP[cmd]
            fn(text, opts)
        except KeyError:
            logger.warning(f"unknown inline command {cmd}")
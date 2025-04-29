from contextlib import closing
import json
import logging
import math
import re
from typing import Generator, Tuple, Any

import boto3
from box import Box, BoxList

from lambda_logs import log_preamble, JSONFormatter, custom_lambda_logs
from substitutions import substitute_job_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


class ConditionFailed(Exception):
    pass


def load_s3_object(repo: str, input_file: str) -> Any:
    if input_file.startswith("s3://"):
        s3_path = input_file
    else:
        s3_path = f"{repo}/{input_file}"

    logger.info(f"loading {s3_path}")
    s3 = boto3.client("s3")

    bucket, key = s3_path.split("/", 3)[2:]
    response = s3.get_object(Bucket=bucket, Key=key)
    with closing(response["Body"]) as fp:
        ret = json.load(fp)

    return ret


def load_vals(inputs_json: str, repo: str) -> Generator[Tuple, None, None]:
    job_data = load_s3_object(repo, "_JOB_DATA_")
    yield "job", job_data["job"]

    inputs = json.loads(inputs_json)
    jobby_inputs = substitute_job_data(inputs, job_data)
    for name, input_file in jobby_inputs.items():
        vals = load_s3_object(repo, input_file)
        yield name, vals

        if len(inputs) == 1 and isinstance(vals, dict):
            vals.pop(name, None)
            for name2, val2 in vals.items():
                yield name2, val2


def eval_this(expr: str, vals: dict):
    result = eval(expr, globals(), vals)
    return result


def run_exprs(exprs: list, vals: dict):
    for expr in exprs:
        result = eval_this(expr, vals)
        logger.info(f"evaluating '{expr}': {result}")
        if result:
            logger.info(f"returning '{expr}'")
            return expr
    logger.info("no conditions evaluated True, returning null")
    return None


def lambda_handler(event: dict, context: object):
    # event = {
    #   repo
    #   inputs -- needs to be a json string for auto inputs compatibility
    #   expressions[] | expression
    #   logging{}
    # }
    # with custom_lambda_logs(**event["logging"]):
    log_preamble(**event.pop("logging"), logger=logger)
    logger.info(f"event: {str(event)}")

    vals = Box(load_vals(event["inputs"], event["repo"]))

    if "expressions" in event:
        ret = run_exprs(event["expressions"], vals)
        return ret

    elif "expression" in event:
        result = eval_this(event["expression"], vals)
        if not result:
            raise ConditionFailed
        return event["expression"]

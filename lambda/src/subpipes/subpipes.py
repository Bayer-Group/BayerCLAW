from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from functools import partial
import json
import logging
import re

import boto3

from lambda_logs import JSONFormatter, custom_lambda_logs
from substitutions import substitute_job_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


def copy_file_impl(spec: str, bucket: str, src_path: str, dst_path: str) -> None:
    try:
        src_file, dst_file = re.split(r"\s*->\s*", spec)
    except ValueError:
        src_file = dst_file = spec

    src_key = f"{src_path}/{src_file}"
    dst_key = f"{dst_path}/{dst_file}"

    logger.info(f"copying s3://{bucket}/{src_key} to s3://{bucket}/{dst_key}")
    s3 = boto3.client("s3")
    copy_source = {"Bucket": bucket, "Key": src_key}
    s3.copy(copy_source, bucket, dst_key)
    logger.info(f"finished copying s3://{bucket}/{src_key} to s3://{bucket}/{dst_key}")


def lambda_handler(event: dict, context: object) -> dict:
    with custom_lambda_logs(**event["logging"]):
        logger.info(str(event))

        # read job data object
        s3 = boto3.client("s3")
        repo_bucket, parent_repo_path = event["repo"].split("/", 3)[2:]
        parent_job_data_key = f"{parent_repo_path}/_JOB_DATA_"
        response = s3.get_object(Bucket=repo_bucket, Key=parent_job_data_key)
        with closing(response["Body"]) as fp:
            parent_job_data = json.load(fp)

        if "submit" in event:
            src_repo_path = parent_repo_path

            # establish subpipe repo
            dst_repo_path = sub_repo_path = f"{parent_repo_path}/{event['logging']['step_name']}"
            logger.info(f"sub repo is {dst_repo_path}")

            # edit job data
            sub_job_data = {
                "job": parent_job_data["job"],
                "parent": {},
                "scatter": {},
            }

            # write job data to subpipe repo
            sub_job_data_key = f"{dst_repo_path}/_JOB_DATA_"
            logger.info(f"writing job data to s3://{repo_bucket}/{sub_job_data_key}")
            s3.put_object(Bucket=repo_bucket, Key=sub_job_data_key, Body=json.dumps(sub_job_data).encode("utf-8"))

            # get submit strings -> spec strings
            spec_strings = json.loads(event["submit"])

        elif "retrieve" in event:
            dst_repo_path = parent_repo_path

            # get subpipe repo
            src_repo_path = sub_repo_path = event["subpipe"]["sub_repo"].split("/", 3)[-1]

            # get retrieve strings -> spec strings
            spec_strings = json.loads(event["retrieve"])

        else:
            raise RuntimeError("unknown input type")

        if spec_strings:
            # substitute job data into spec strings
            subbed_specs = substitute_job_data(spec_strings, parent_job_data)

            # copy files from src repo to dest
            with ThreadPoolExecutor(max_workers=len(subbed_specs)) as executor:
                copy_file = partial(copy_file_impl,
                                    bucket=repo_bucket,
                                    src_path=src_repo_path,
                                    dst_path=dst_repo_path)
                _ = list(executor.map(copy_file, subbed_specs))

        # return dest repo
        ret = {"sub_repo": f"s3://{repo_bucket}/{sub_repo_path}"}

        return ret

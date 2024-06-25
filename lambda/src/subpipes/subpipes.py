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


def get_s3_object(s3_uri: str) -> dict:
    logger.info(f"reading {s3_uri}")
    bucket, key = s3_uri.split("/", 3)[2:]
    obj = boto3.resource("s3").Object(bucket, key)
    response = obj.get()
    with closing(response["Body"]) as fp:
        ret = json.load(fp)
    return ret


def put_s3_object(s3_uri: str, body: bytes) -> None:
    logger.info(f"writing {s3_uri}")
    bucket, key = s3_uri.split("/", 3)[2:]
    obj = boto3.resource("s3").Object(bucket, key)
    obj.put(Body=body)


def copy_file_impl(spec: str, src_repo_uri: str, dst_repo_uri: str) -> None:
    try:
        src_file, dst_file = re.split(r"\s*->\s*", spec)
    except ValueError:
        src_file = dst_file = spec

    src_uri = f"{src_repo_uri}/{src_file}"
    dst_uri = f"{dst_repo_uri}/{dst_file}"

    src_bucket, src_key = src_uri.split("/", 3)[2:]
    dst_bucket, dst_key = dst_uri.split("/", 3)[2:]

    logger.info(f"copying s3://{src_bucket}/{src_key} to s3://{dst_bucket}/{dst_key}")

    copy_src = {
        "Bucket": src_bucket,
        "Key": src_key
    }
    dst_obj = boto3.resource("s3").Object(dst_bucket, dst_key)
    dst_obj.copy(copy_src)


def lambda_handler(event: dict, context: object) -> dict:
    # submit event = {
    #   repo: str
    #   job_data: str | None
    #   submit: json-formatted list of str
    #   logging: {}
    # }
    # retrieve event = {
    #   repo: str
    #   retrieve: json-formatted list of str
    #   subpipe: {
    #     sub_repo: str
    #   }
    #   logging: {}
    # }

    with custom_lambda_logs(**event["logging"]):
        logger.info(str(event))

        parent_repo = event["repo"]
        parent_job_data = get_s3_object(f"{parent_repo}/_JOB_DATA_")

        if "submit" in event:
            # establish subpipe repo
            sub_repo = f"{parent_repo}/{event['logging']['step_name']}"
            logger.info(f"{sub_repo=}")

            if (sub_job_data_uri := event.get("job_data")) is not None:
                if not sub_job_data_uri.startswith("s3://"):
                    sub_job_data_uri = f"{parent_repo}/{sub_job_data_uri}"
                logger.info(f"{sub_job_data_uri=}")
                sub_job_data = get_s3_object(sub_job_data_uri)

            else:
                logger.info("using parent job data for subpipe")
                sub_job_data = parent_job_data["job"]

            # create job data for subpipe
            sub_job_data = {
                "job": sub_job_data,
                "parent": {},
                "scatter": {},
            }

            # write job data to subpipe repo
            sub_job_data_dst = f"{sub_repo}/_JOB_DATA_"
            logger.info(f"writing job data to {sub_job_data_dst}")
            put_s3_object(sub_job_data_dst, json.dumps(sub_job_data).encode("utf-8"))

            # get submit strings -> spec strings
            spec_strings = json.loads(event["submit"])

            src_repo_uri = parent_repo
            dst_repo_uri = sub_repo

        elif "retrieve" in event:
            # get retrieve strings -> spec strings
            spec_strings = json.loads(event["retrieve"])

            # get subpipe repo
            src_repo_uri = sub_repo = event["subpipe"]["sub_repo"]
            dst_repo_uri = parent_repo

        else:
            raise RuntimeError("unknown input type")

        if spec_strings:
            # substitute job data into spec strings
            subbed_specs = substitute_job_data(spec_strings, parent_job_data)

            # copy files from src repo to dest
            with ThreadPoolExecutor(max_workers=len(subbed_specs)) as executor:
                copy_file = partial(copy_file_impl,
                                    src_repo_uri=src_repo_uri,
                                    dst_repo_uri=dst_repo_uri)
                _ = list(executor.map(copy_file, subbed_specs))

        # return sub repo
        sub_repo_bucket, sub_repo_prefix = sub_repo.split("/", 3)[2:]
        ret = {
            "sub_repo": {
                "bucket": sub_repo_bucket,
                "prefix": sub_repo_prefix,
                "uri": sub_repo,
            }
        }

        return ret

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from functools import partial
import jmespath
import json
import logging
from typing import Generator

import boto3
from botocore.exceptions import ClientError

from lambda_logs import JSONFormatter, custom_lambda_logs
from substitutions import substitute_job_data, substitute_into_filenames

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


def _output_path_generator(filename: str, repos: list) -> Generator[str, None, None]:
    session = boto3.Session()
    s3 = session.resource("s3")

    for repo in repos:
        src_path = f"{repo}/{filename}"
        bucket, obj_key = src_path.split("/", 3)[2:]
        try:
            s3.Object(bucket, obj_key).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning(f"{filename} not found in {repo}")
            else:
                raise
        else:
            yield src_path


def find_output_files(kv: (str, str), repos: list) -> (str, list):
    key, filename = kv
    ret = list(_output_path_generator(filename, repos))
    if len(ret) == 0:
        logger.warning(f"no files named {filename} found")

    return key, ret


def lambda_handler(event: dict, context):
    with custom_lambda_logs(**event["logging"]):
        logger.info(json.dumps(event))
    
        parent_repo = event["repo"]
        parent_job_data_path = f"{parent_repo}/_JOB_DATA_"
        parent_job_data_bucket, parent_job_data_key = parent_job_data_path.split("/", 3)[2:]

        parent_outputs = json.loads(event["outputs"])
        step_name = event["logging"]["step_name"]

        response = boto3.resource("s3").Object(parent_job_data_bucket, parent_job_data_key).get()
        with closing(response["Body"]) as fp:
            parent_job_data = json.load(fp)

        jobby_outputs = substitute_job_data(parent_outputs, parent_job_data)

        ret = {}

        if len(jobby_outputs) > 0:
            # repos = jmespath.search("results[].repo", event)
            repos = jmespath.search("items[].repo", event)
            finder = partial(find_output_files, repos=repos)

            with ThreadPoolExecutor(max_workers=len(jobby_outputs)) as executor:
                manifest = dict(executor.map(finder, jobby_outputs.items()))

            manifest_filename = f"{step_name}_manifest.json"
            manifest_path = f"{parent_repo}/{manifest_filename}"
            manifest_bucket, manifest_key = manifest_path.split("/", 3)[2:]
            manifest_obj = boto3.resource("s3").Object(manifest_bucket, manifest_key)
            manifest_obj.put(Body=json.dumps(manifest).encode("utf-8"))

            ret["manifest"] = manifest_filename

        return ret

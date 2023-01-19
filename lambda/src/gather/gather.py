from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from functools import partial
from itertools import groupby
import jmespath
import json
import logging
from os.path import basename
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


def find_this_file(file_name: str, bucket_name: str, objs: list) -> Generator[str, None, None]:
    for obj in objs:
        if obj.key.endswith("/" + file_name):
            yield f"s3://{bucket_name}/{obj.key}"


def manifest_entries(output_files: dict, bucket, prefix: str) -> Generator[tuple, None, None]:
    scatter_outputs = bucket.objects.filter(Prefix=prefix)
    for key, filename in output_files.items():
        files = list(find_this_file(filename, bucket.name, scatter_outputs))
        if not files:
            logger.warning(f"no files named {filename} found")
        yield key, files


def lambda_handler(event: dict, context: object):
    with custom_lambda_logs(**event["logging"]):
        logger.info(json.dumps(event))

        parent_outputs = json.loads(event["outputs"])
        if parent_outputs:
            step_name = event["logging"]["step_name"]

            parent_repo = event["repo"]
            parent_repo_bucket, parent_repo_prefix = parent_repo.split("/", 3)[2:]
            parent_job_data_key = f"{parent_repo_prefix}/_JOB_DATA_"

            response = boto3.resource("s3").Object(parent_repo_bucket, parent_job_data_key).get()
            with closing(response["Body"]) as fp:
                parent_job_data = json.load(fp)

            jobby_outputs = substitute_job_data(parent_outputs, parent_job_data)

            bucket = boto3.resource("s3").Bucket(parent_repo_bucket)
            prefix = f"{parent_repo_prefix}/{step_name}"
            scatter_output_objs = bucket.objects.filter(Prefix=prefix)
            scatter_output_uris = [f"s3://{parent_repo_bucket}/{o.key}" for o in scatter_output_objs]
            scatter_output_uris.sort(key=basename)
            thang = {k: list(g) for k, g in groupby(scatter_output_uris, key=basename)}
            manifest = {}
            for k, v in jobby_outputs.items():
                if v in thang:
                    manifest[k] = thang[v]
                else:
                    logger.warning(f"no files named {v} found")
                    manifest[k] = []

            manifest_filename = f"{step_name}_manifest.json"
            manifest_path = f"{parent_repo}/{manifest_filename}"
            manifest_bucket, manifest_key = manifest_path.split("/", 3)[2:]
            manifest_obj = boto3.resource("s3").Object(manifest_bucket, manifest_key)
            manifest_obj.put(Body=json.dumps(manifest).encode("utf-8"))

            ret = {"manifest": manifest_filename}
        else:
            ret = {}

        return ret


def lambda_handler1(event: dict, context: object):
    with custom_lambda_logs(**event["logging"]):
        logger.info(json.dumps(event))

        parent_outputs = json.loads(event["outputs"])
        if parent_outputs:
            step_name = event["logging"]["step_name"]

            parent_repo = event["repo"]
            parent_repo_bucket, parent_repo_prefix = parent_repo.split("/", 3)[2:]
            parent_job_data_key = f"{parent_repo_prefix}/_JOB_DATA_"

            response = boto3.resource("s3").Object(parent_repo_bucket, parent_job_data_key).get()
            with closing(response["Body"]) as fp:
                parent_job_data = json.load(fp)

            jobby_outputs = substitute_job_data(parent_outputs, parent_job_data)

            bucket = boto3.resource("s3").Bucket(parent_repo_bucket)
            prefix = f"{parent_repo_prefix}/{step_name}"
            manifest = dict(manifest_entries(jobby_outputs, bucket, prefix))

            manifest_filename = f"{step_name}_manifest.json"
            manifest_path = f"{parent_repo}/{manifest_filename}"
            manifest_bucket, manifest_key = manifest_path.split("/", 3)[2:]
            manifest_obj = boto3.resource("s3").Object(manifest_bucket, manifest_key)
            manifest_obj.put(Body=json.dumps(manifest).encode("utf-8"))

            ret = {"manifest": manifest_filename}
        else:
            ret = {}

        return ret


def lambda_handler0(event: dict, context):
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

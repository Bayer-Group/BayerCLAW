from contextlib import closing
from itertools import groupby
import json
import logging
from os.path import basename

import boto3

from lambda_logs import log_preamble, log_event
from substitutions import substitute_job_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context: object):
    # event = {
    #   outputs: str
    #   repo: str
    #   step_name: str
    #   logging: {
    #     branch: str
    #     job_file_bucket: str
    #     job_file_key: str
    #     job_file_version: str
    #     sfn_execution_id: str
    #     step_name: str
    #     workflow_name: str
    #   }
    # }

    log_preamble(**event["logging"], logger=logger)
    log_event(logger, event)

    parent_outputs = json.loads(event["outputs"])
    if parent_outputs:
        step_name = event["step_name"]

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
        scatter_output_uris = [f"s3://{o.bucket_name}/{o.key}" for o in scatter_output_objs]
        scatter_output_uris.sort(key=basename)

        filename2group = {k: list(g) for k, g in groupby(scatter_output_uris, key=basename)}
        manifest = {}
        for key, filename in jobby_outputs.items():
            if filename in filename2group:
                manifest[key] = filename2group[filename]
            else:
                logger.warning(f"no files named {filename} found")
                manifest[key] = []

        manifest_filename = f"{step_name}_manifest.json"
        manifest_path = f"{parent_repo}/{manifest_filename}"
        manifest_bucket, manifest_key = manifest_path.split("/", 3)[2:]
        manifest_obj = boto3.resource("s3").Object(manifest_bucket, manifest_key)
        manifest_obj.put(Body=json.dumps(manifest).encode("utf-8"))

        ret = {"manifest": manifest_filename}
    else:
        ret = {}

    return ret

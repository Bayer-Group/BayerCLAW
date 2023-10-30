from contextlib import closing
import json
import logging

import boto3

from lambda_logs import JSONFormatter, custom_lambda_logs
from repo_utils import Repo, RepoEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


def lambda_handler(event: dict, context: object):
    # event = {
    #   index: str,
    #   repo: str
    #   scatter: {
    #     key: value
    #   }
    #   logging: {}
    #   ...
    # }

    with custom_lambda_logs(**event["logging"]):
        logger.info(f"{event=}")

        s3 = boto3.resource("s3")

        # read job data template
        scatter_repo = Repo.from_uri(event["repo"])

        job_data_template = scatter_repo.job_data_file
        obj = s3.Object(job_data_template.bucket, job_data_template.key)
        response = obj.get()
        with closing(response["Body"]) as fp:
            job_data = json.load(fp)

        # replace scatter field
        job_data["scatter"].update(event["scatter"])

        # establish branch repo
        branch_repo = scatter_repo.sub_repo(f"{int(event['index']):05}")

        # write job data
        job_data_file = branch_repo.job_data_file
        job_data_obj = s3.Object(job_data_file.bucket, job_data_file.key)
        job_data_obj.put(Body=json.dumps(job_data, cls=RepoEncoder).encode("utf-8"),
                         ServerSideEncryption="AES256")

        # return repo uri
        return str(branch_repo)

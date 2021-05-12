from contextlib import closing
import fnmatch
import itertools
import jmespath
import json
import logging
import re
from typing import Dict, Any, Generator

import boto3

from file_select import select_file_contents
from lambda_logs import JSONFormatter, custom_lambda_logs
from substitutions import substitute_job_data, substitute_into_filenames

SESSION = boto3.Session()
S3 = SESSION.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


def prepend_repo(file: str, repo: str) -> str:
    if file.startswith("s3://"):
        return file
    else:
        return f"{repo}/{file}"


def expand_glob(glob: str, repo: str) -> list:
    glob = prepend_repo(glob, repo)

    bucket_name, globby_s3_key = glob.split('/', 3)[2:]

    # get the invariant part of the s3 key glob
    #   example: dir1/dir2/dir*/yada_yada.txt --> dir1/dir2
    # this will be used to limit the number of s3 objects to search
    prefix = re.search(r'^([^\[\]*?]+)(?=/)', globby_s3_key).group(0)

    bucket = SESSION.resource("s3").Bucket(bucket_name)
    object_summaries = bucket.objects.filter(Prefix=prefix)
    object_keys = [o.key for o in object_summaries]

    target_keys = fnmatch.filter(object_keys, globby_s3_key)
    target_paths = [f"s3://{bucket_name}/{k}" for k in target_keys]

    return target_paths


# cases:
#   1. static list
#   2. list in a job data field
#   3. file contents
#   4. file glob
#   5. single filename
def expand_scatter_data(scatter_spec: dict, repo: str, job_data: dict) -> Dict[Any, list]:
    ret = dict()
    for key, vals in scatter_spec.items():
        if isinstance(vals, list):
            ret[key] = vals

        elif isinstance(vals, str):
            is_job_data = re.match(r"^\${((?:job|parent|scatter)\..+?)}$", vals)
            if is_job_data is not None:
                ret[key] = jmespath.search(is_job_data.group(1), job_data)

            elif vals.startswith("@"):
                path = prepend_repo(vals[1:], repo)
                ret[key] = select_file_contents(path)

            elif re.search(r'[\[\]*?]', vals):
                ret[key] = expand_glob(vals, repo)

            else:
                # assume vals contains a single filename
                ret[key] = [prepend_repo(vals, repo)]
        else:
            pass
    return ret


def scatterator(scatter_data: Dict[Any, list]) -> Generator[dict, None, None]:
    keys = scatter_data.keys()
    vals = scatter_data.values()
    for p in itertools.product(*vals):
        yield dict(zip(keys, p))


def lambda_handler(event: dict, context: object):
    with custom_lambda_logs(**event["logging"]):
        logger.info(json.dumps(event))

        parent_repo = event["repo"]
        parent_job_data_path = f"{parent_repo}/_JOB_DATA_"
        parent_job_data_bucket, parent_job_data_key = parent_job_data_path.split("/", 3)[2:]

        parent_inputs = json.loads(event["inputs"])
        scatter_data = json.loads(event["scatter"])
        step_name = event["logging"]["step_name"]

        response = S3.get_object(Bucket=parent_job_data_bucket, Key=parent_job_data_key)
        with closing(response["Body"]) as fp:
            parent_job_data = json.load(fp)

        jobby_inputs = substitute_job_data(parent_inputs, parent_job_data)
        repoized_inputs = {k: prepend_repo(v, parent_repo) for k, v in jobby_inputs.items()}

        expanded_scatter = expand_scatter_data(scatter_data, parent_repo, parent_job_data)

        child_repo_home = f"{parent_repo}/{step_name}"
        ret = []

        for i, combo in enumerate(scatterator(expanded_scatter), start=0):
            curr_repo = f"{child_repo_home}/{i:05}"

            curr_job_data = {
                "job": parent_job_data["job"],
                "scatter": combo,
                "parent": {**parent_job_data["parent"], **repoized_inputs}
            }

            curr_job_data_s3_path = f"{curr_repo}/_JOB_DATA_"
            curr_job_data_bucket, curr_job_data_key = curr_job_data_s3_path.split("/", 3)[2:]

            S3.put_object(Bucket=curr_job_data_bucket, Key=curr_job_data_key,
                          Body=json.dumps(curr_job_data).encode("utf-8"),
                          ServerSideEncryption="AES256")

            ret.append({
                "repo": curr_repo,
            })

        return ret

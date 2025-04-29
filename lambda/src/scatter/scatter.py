from contextlib import closing
import csv
import fnmatch
import itertools
import json
import logging
import re
from typing import Any, Dict, Generator, Tuple

import boto3
import jmespath

from file_select import select_file_contents
from lambda_logs import log_preamble, log_event
from repo_utils import Repo, S3File
from substitutions import substitute_job_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_job_data(repo: Repo) -> dict:
    job_data = repo.qualify("_JOB_DATA_")
    obj = boto3.resource("s3").Object(job_data.bucket, job_data.key)
    response = obj.get()
    with closing(response["Body"]) as fp:
        ret = json.load(fp)
    return ret


def expand_glob(globby_file: S3File) -> Generator[S3File, None, None]:
    # get the invariant part of the s3 key glob
    #   example: dir1/dir2/dir*/yada_yada.txt --> dir1/dir2/dir
    # this will be used to limit the number of s3 objects to search
    prefix = re.search(r'(^.*?)[\[\]*?]+', globby_file.key).group(1)

    bucket = boto3.resource("s3").Bucket(globby_file.bucket)
    object_summaries = bucket.objects.filter(Prefix=prefix)
    object_keys = [o.key for o in object_summaries]

    target_keys = fnmatch.filter(object_keys, globby_file.key)

    for key in target_keys:
        yld = S3File(globby_file.bucket, key)
        yield yld


def expand_scatter_data(scatter_spec: dict, repo: Repo, job_data: dict) -> Generator[Tuple, None, None]:
    for key, vals in scatter_spec.items():
        yield from _expand_scatter_data_impl(key, vals, repo, job_data)


def _expand_scatter_data_impl(key: str, vals: Any, repo: Repo, job_data: dict) -> Generator[Tuple, None, None]:
    logger.info(f"expanding: {vals=}")

    # case 1: static list
    if isinstance(vals, list):
        yield key, vals

    elif isinstance(vals, str):
        # case 2: reference to something in an extended job data file field

        # it is not likely that anything will be found in the parent or scatter fields of the job data
        # object, since they will be populated only inside of a scatter step and nested scatters are
        # not allowed (yet). This may change in future versions.
        if (is_job_data_ref := re.search(r"\${((?:job|parent|scatter)\..+?)}", vals)) is not None:
            field_name = is_job_data_ref.group(1)  # e.g. "job.list_of_stuff"

            result = jmespath.search(field_name, job_data)

            if result is None:
                raise RuntimeError(f"{key=}: {field_name} not found")

            # assume:
            #   - if the jmespath search yielded a list, you want to scatter on that list
            #   - if the jmespath search yielded a string, you want to substitute that string into vals
            #     and expand from that
            # anything else will raise an error
            if isinstance(result, list):
                pass

            elif isinstance(result, str):
                # don't search the vals string twice
                result = vals[:is_job_data_ref.start(0)] + result + vals[is_job_data_ref.end(0):]

            else:
                raise RuntimeError(f"{key=}: {field_name} is not a list or string")

        # case 3: file contents
        elif vals.startswith("@"):
            path = repo.qualify(vals[1:])
            result = select_file_contents(str(path))

        # case 4: file glob
        elif re.search(r'[\[\]*?]', vals):
            result = list(expand_glob(repo.qualify(vals)))

        # case 5: single filename
        else:
            result = [repo.qualify(vals)]

        yield from _expand_scatter_data_impl(key, result, repo, job_data)

    else:
        pass


def scatterator(scatter_data: Dict[Any, list]) -> Generator[dict, None, None]:
    keys = scatter_data.keys()
    vals = scatter_data.values()
    for p in itertools.product(*vals):
        combo = dict(zip(keys, p))
        yield combo


def write_job_data_template(parent_job_data: dict,
                            repoized_inputs: dict,
                            jobby_outputs: dict,
                            scatter_repo: Repo) -> S3File:
    job_data_template = {
        "job": parent_job_data["job"],
        "scatter": {},
        "parent": {**parent_job_data["parent"], **repoized_inputs, **jobby_outputs},
    }
    template_file = scatter_repo.qualify("_JOB_DATA_")
    template_obj = boto3.resource("s3").Object(template_file.bucket, template_file.key)
    template_obj.put(Body=json.dumps(job_data_template).encode("utf-8"),
                     ServerSideEncryption="AES256")
    return template_file


def lambda_handler(event: dict, context: object):
    # event = {
    #   repo: {
    #       bucket: str
    #       prefix: str
    #   }
    #   inputs: str
    #   outputs: str
    #   scatter: str
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

    log_preamble(**event.pop("logging"), logger=logger)
    log_event(logger, event)

    parent_repo = Repo(event["repo"])
    parent_job_data = get_job_data(parent_repo)

    parent_inputs = json.loads(event["inputs"])
    parent_outputs = json.loads(event["outputs"])
    scatter_data = json.loads(event["scatter"])
    step_name = event["step_name"]

    scatter_repo = parent_repo.sub_repo(step_name)

    jobby_inputs = substitute_job_data(parent_inputs, parent_job_data)
    jobby_outputs = substitute_job_data(parent_outputs, parent_job_data)
    repoized_inputs = {k: parent_repo.qualify(v) for k, v in jobby_inputs.items()}
    _ = write_job_data_template(parent_job_data, repoized_inputs, jobby_outputs, scatter_repo)

    expanded_scatter_data = dict(expand_scatter_data(scatter_data, parent_repo, parent_job_data))

    with open("/tmp/items.csv", "w") as fp:
        writer = csv.DictWriter(fp, fieldnames=expanded_scatter_data.keys(), dialect="unix")
        writer.writeheader()
        writer.writerows(scatterator(expanded_scatter_data))

    items_file = scatter_repo.qualify("items.csv")
    items_obj = boto3.resource("s3").Object(items_file.bucket, items_file.key)
    items_obj.upload_file("/tmp/items.csv")

    ret = {
        "items": {
            "bucket": items_file.bucket,
            "key": items_file.key
        },
        "repo": dict(scatter_repo),
    }
    return ret

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import fnmatch
from functools import partial
import glob as g
import json
import logging
import os
import re
from typing import Dict, Generator, Iterable, List

import backoff
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def _is_glob(filename: str) -> bool:
    ret = re.search(r"[\[\]?*]", filename)
    return ret is not None


@backoff.on_exception(backoff.expo, ClientError, max_time=60)
def _s3_file_exists(key: str, bucket: str) -> bool:
    session = boto3.Session()
    s3 = session.resource("s3")
    obj = s3.Object(bucket, key)
    try:
        obj.load()
        logger.info(f"s3://{bucket}/{key} exists")
        return True
    except ClientError as ce:
        if ce.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            logger.info(f"s3://{bucket}/{key} does not exist")
            return False
        else:
            raise


def _expand_s3_glob(glob: str) -> Generator[str, None, None]:
    bucket_name, globby_s3_key = glob.split("/", 3)[2:]
    prefix = re.search(r"^([^\[\]*?]+)(?=/)", globby_s3_key).group(0)

    session = boto3.Session()
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    object_summaries = bucket.objects.filter(Prefix=prefix)
    object_keys = [o.key for o in object_summaries]

    target_keys = fnmatch.filter(object_keys, globby_s3_key)
    target_paths = (f"s3://{bucket_name}/{k}" for k in target_keys)
    yield from target_paths


def _inputerator(s3_paths: Iterable[str]) -> Generator[str, None, None]:
    for s3_path in s3_paths:
        if _is_glob(s3_path):
            yield from _expand_s3_glob(s3_path)
        else:
            yield s3_path


def _download_this(s3_path: str, optional: bool) -> None:
    bucket, key = s3_path.split("/", 3)[2:]
    filename = os.path.basename(key)
    logger.info(f"starting download: {s3_path} -> {filename}")
    session = boto3.Session()
    s3 = session.resource("s3")
    try:
        s3.Object(bucket, key).download_file(filename)
        logger.info(f"finished download: {s3_path} -> {filename}")
    except Exception as e:
        if optional and "Not Found" in str(e):
            logger.warning(f"optional file not found: {s3_path}; skipping")
        else:
            raise RuntimeError(f"download failed: {s3_path}\nreason: {str(e)}")


def _outputerator(output_files: Iterable[str]) -> Generator[str, None, None]:
    for file in output_files:
        expanded = g.glob(file)
        if not expanded:
            logger.warning(f"no file matching '{file}' found in workspace")
        else:
            for f in expanded:
                yield f


def _upload_that(local_file: str, bucket: str, prefix: str) -> None:
    key = f"{prefix}/{os.path.basename(local_file)}"
    logger.info(f"starting upload: {local_file} -> s3://{bucket}/{key}")
    session = boto3.Session()
    s3 = session.resource("s3")
    try:
        s3.Object(bucket, key).upload_file(local_file,
                                           ExtraArgs={"ServerSideEncryption": "AES256",
                                                      "Metadata": {"execution_id": os.environ.get("BC_EXECUTION_ID", "undefined")}})
        logger.info(f"finished upload: {local_file} -> s3://{bucket}/{key}")
    except FileNotFoundError:
        logger.warning(f"{local_file} not found; skipping upload")
    except Exception as e:
        raise RuntimeError(f"upload failed: {os.path.basename(local_file)} -> s3://{bucket}/{key}\nreason: {str(e)}")


class Repository(object):
    def __init__(self, path: str):
        self.full_path = path
        self.bucket, self.prefix = path.split("/", 3)[2:]
        self.run_status_obj = f"{self.prefix}/_control_/{os.environ['BC_STEP_NAME']}.complete"

    def read_job_data(self) -> dict:
        s3 = boto3.client("s3")
        key = f"{self.prefix}/_JOB_DATA_"
        response = s3.get_object(Bucket=self.bucket, Key=key)
        with closing(response["Body"]) as fp:
            ret = json.load(fp)
        return ret

    def add_s3_path(self, name: str) -> str:
        if name.startswith("s3://"):
            ret = name
        else:
            ret = f"{self.full_path}/{os.path.basename(name)}"
        return ret

    def files_exist(self, filenames: List[str]) -> bool:
        # this is for backward compatibility. Note that if you have a step that produces
        # no outputs (i.e. being run for side effects only), it will always be skipped
        # if run with skip_if_files_exist
        if len(filenames) == 0:
            return True

        # there's no way to know if all the files included in a glob were uploaded in
        # a previous run, so just return False to be safe
        if any(_is_glob(f) for f in filenames):
            return False

        keys = [f"{self.prefix}/{f}" for f in filenames]
        checker = partial(_s3_file_exists, bucket=self.bucket)

        with ThreadPoolExecutor(max_workers=len(keys)) as executor:
            ret = all(executor.map(checker, keys))

        return ret

    def download_inputs(self, input_spec: Dict[str, str], optional: bool) -> Dict[str, str]:
        if optional:
            logger.info(f"downloading {len(input_spec)} optional file(s) from S3")
        else:
            logger.info(f"downloading {len(input_spec)} required file(s) from S3")

        s3_paths = {k: self.add_s3_path(v) for k, v in input_spec.items()}
        expanded_s3_paths = list(_inputerator(s3_paths.values()))
        if len(expanded_s3_paths) > 0:
            downloader = partial(_download_this, optional=optional)
            with ThreadPoolExecutor(max_workers=min(len(expanded_s3_paths), 256)) as executor:
                _ = list(executor.map(downloader, expanded_s3_paths))

        ret = {k: os.path.basename(v) for k, v in input_spec.items()}
        return ret

    def upload_outputs(self, output_spec: Dict[str, str]) -> None:
        expanded_local_files = list(_outputerator(output_spec.values()))
        if len(expanded_local_files) > 0:
            uploader = partial(_upload_that, bucket=self.bucket, prefix=self.prefix)
            with ThreadPoolExecutor(max_workers=min(len(expanded_local_files), 256)) as executor:
                _ = list(executor.map(uploader, expanded_local_files))

    @backoff.on_exception(backoff.expo, RuntimeError, max_time=60)
    def check_for_previous_run(self) -> bool:
        """
        Returns:
            True if this step has been run before
        """
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.run_status_obj)
        if not 200 <= response["ResponseMetadata"]["HTTPStatusCode"] <= 299:
            raise RuntimeError("unable to query previous run status")
        return response["KeyCount"] > 0

    @backoff.on_exception(backoff.expo, RuntimeError, max_time=60)
    def clear_run_status(self) -> None:
        s3 = boto3.client("s3")
        response = s3.delete_object(Bucket=self.bucket, Key=self.run_status_obj)
        if not 200 <= response["ResponseMetadata"]["HTTPStatusCode"] <= 299:
            raise RuntimeError("unable to clear previous run status")

    @backoff.on_exception(backoff.expo, RuntimeError, max_time=60)
    def put_run_status(self) -> None:
        s3 = boto3.client("s3")
        response = s3.put_object(Bucket=self.bucket, Key=self.run_status_obj, Body=b"")
        if not 200 <= response["ResponseMetadata"]["HTTPStatusCode"] <= 299:
            raise RuntimeError("failed to upload run status")

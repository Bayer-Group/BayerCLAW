from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import fnmatch
import glob as g
import json
import logging
import os
import re
from typing import Dict, Generator, Iterable, List

import boto3
import botocore.exceptions
from more_itertools import peekable

logger = logging.getLogger(__name__)


def _file_metadata():
    ret = {"execution_id": os.environ.get("BC_EXECUTION_ID", "undefined")}
    return ret


def _is_glob(filename: str) -> bool:
    ret = re.search(r"[\[\]?*]", filename)
    return ret is not None


def _expand_s3_glob(glob: str) -> Generator[str, None, None]:
    bucket_name, globby_s3_key = glob.split("/", 3)[2:]
    prefix = re.search(r"^([^\[\]*?]+)(?=/)", globby_s3_key).group(0)

    session = boto3.Session()
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    object_summaries = bucket.objects.filter(Prefix=prefix)
    object_keys = (o.key for o in object_summaries)

    target_keys = fnmatch.filter(object_keys, globby_s3_key)
    target_paths = (f"s3://{bucket_name}/{k}" for k in target_keys)
    yield from target_paths


class Repository(object):
    def __init__(self, s3_uri: str):
        self.s3_uri = s3_uri
        self.bucket, self.prefix = s3_uri.split("/", 3)[2:]
        self.run_status_obj = f"_control_/{os.environ['BC_STEP_NAME']}.complete"

    def to_uri(self, filename: str) -> str:
        ret = f"{self.s3_uri}/{filename}"
        return ret

    def qualify(self, filename: str) -> str:
        ret = f"{self.prefix}/{filename}"
        return ret

    def read_job_data(self) -> dict:
        s3 = boto3.resource("s3")
        job_data_obj = s3.Object(self.bucket, self.qualify("_JOB_DATA_"))
        response = job_data_obj.get()
        with closing(response["Body"]) as fp:
            ret = json.load(fp)
        return ret

    # todo: need test
    def _s3_file_exists(self, key: str) -> bool:
        session = boto3.Session()
        s3 = session.resource("s3")
        obj = s3.Object(self.bucket, key)
        try:
            obj.load()
            logger.info(f"s3://{self.bucket}/{key} exists")
            return True
        except botocore.exceptions.ClientError as ce:
            if ce.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                logger.info(f"s3://{self.bucket}/{key} does not exist")
                return False
            else:
                raise

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

        keys = (self.qualify(os.path.basename(f)) for f in filenames)
        ret = all(self._s3_file_exists(k) for k in keys)
        return ret

    def _inputerator(self, input_spec: Dict[str, str]) -> Generator[str, None, None]:
        for symbolic_name, filename in input_spec.items():
            optional = symbolic_name.endswith("?")

            if filename.startswith("s3://"):
                uri = filename
            else:
                uri = self.to_uri(os.path.basename(filename))

            s3_objects = peekable(_expand_s3_glob(uri))
            if not s3_objects:
                if optional:
                    logger.warning(f"optional file not found: {filename}; skipping")
                else:
                    raise FileNotFoundError(filename)
            yield from s3_objects

    @staticmethod
    def _download_this(s3_uri: str) -> str:
        bucket, key = s3_uri.split("/", 3)[2:]
        dest = os.path.basename(key)
        logger.info(f"starting download: {s3_uri} -> {dest}")
        try:
            session = boto3.Session()
            s3 = session.resource("s3")
            s3.Object(bucket, key).download_file(dest)
            logger.info(f"finished download: {s3_uri} -> {dest}")
            return dest
        except botocore.exceptions.ClientError as ce:
            if "Not Found" in str(ce):
                raise FileNotFoundError(s3_uri)
            else:
                raise

    def download_inputs(self, input_spec: Dict[str, str]) -> Dict[str, str]:
        with ThreadPoolExecutor(max_workers=256) as executor:
            result = list(executor.map(self._download_this, self._inputerator(input_spec)))

        logger.info(f"{len(result)} files downloaded")

        ret = {k.rstrip("?"): os.path.basename(v) for k, v in input_spec.items()}
        return ret

    @staticmethod
    def _outputerator(output_files: Iterable[str]) -> Generator[str, None, None]:
        for file in output_files:
            expanded = g.glob(file, recursive=True)
            if not expanded:
                logger.warning(f"no file matching '{file}' found in workspace")
            yield from expanded

    def _upload_that(self, local_file: str) -> str:
        key = self.qualify(os.path.basename(local_file))
        dest = self.to_uri(os.path.basename(local_file))
        logger.info(f"starting upload: {local_file} -> {dest}")
        session = boto3.Session()
        s3 = session.resource("s3")
        s3.Object(self.bucket, key).upload_file(local_file,
                                           ExtraArgs={"ServerSideEncryption": "AES256",
                                                      "Metadata": _file_metadata()})
        logger.info(f"finished upload: {local_file} -> {dest}")
        return dest

    def upload_outputs(self, output_spec: Dict[str, str]) -> None:
        with ThreadPoolExecutor(max_workers=256) as executor:
            result = list(executor.map(self._upload_that, self._outputerator(output_spec.values())))
        logger.info(f"{len(result)} files uploaded")

    def check_for_previous_run(self) -> bool:
        """
        Returns:
            True if this step has been run before
        """
        ret = self._s3_file_exists(self.qualify(self.run_status_obj))
        return ret

    def clear_run_status(self) -> None:
        s3 = boto3.resource("s3")
        status_obj = s3.Object(self.bucket, self.qualify(self.run_status_obj))
        status_obj.delete()

    def put_run_status(self) -> None:
        s3 = boto3.resource("s3")
        status_obj = s3.Object(self.bucket, self.qualify(self.run_status_obj))
        status_obj.put(Body=b"", Metadata=_file_metadata())

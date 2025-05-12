from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import fnmatch
import glob as g
import json
import logging
import os
import re
from typing import Dict, Generator, Iterable, List, Tuple

import boto3
import botocore.exceptions
from more_itertools import peekable

logger = logging.getLogger(__name__)


class SkipExecution(Exception):
    pass


def _file_metadata():
    ret = {"execution_id": os.environ.get("BC_EXECUTION_ID", "undefined")}
    return ret


def _is_glob(filename: str) -> bool:
    ret = re.search(r"[\[\]?*]", filename)
    return ret is not None


def _expand_s3_glob(glob: str) -> Generator[str, None, None]:
    bucket_name, globby_s3_key = glob.split("/", 3)[2:]
    if (m := re.search(r"^([^\[\]*?]+)(?=/)", globby_s3_key)) is not None:
        prefix = m.group(0)
    else:
        prefix = ""

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
        logger.info(f"repository={s3_uri}")
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


    def check_files_exist(self, filenames: List[str]) -> None:
        """
        Raises SkipExecution if this step has been run before
        """
        logger.info("checking for existing output files")

        # this is for backward compatibility. Note that if you have a step that produces
        # no outputs (i.e. being run for side effects only), it will always be skipped
        # if run with skip_if_files_exist
        # if len(filenames) == 0:
        #    raise SkipExecution("found output files; skipping")

        # there's no way to know if all the files included in a glob were uploaded in
        # a previous run, so always rerun to be safe
        if any(_is_glob(f) for f in filenames):
            return

        # note: all([]) = True
        keys = (self.qualify(os.path.basename(f)) for f in filenames)
        if all(self._s3_file_exists(k) for k in keys):
            raise SkipExecution("found output files; skipping")

        logger.info("output files missing; continuing")


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
        session = boto3.Session()
        s3 = session.resource("s3")
        try:
            s3_obj = s3.Object(bucket, key)
            s3_size = s3_obj.content_length
            logger.info(f"starting download: {s3_uri} ({s3_size} bytes) -> {dest}")
            s3_obj.download_file(dest)
            local_size = os.path.getsize(dest)
            logger.info(f"finished download: {s3_uri} ({s3_size} bytes) -> {dest} ({local_size} bytes)")
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
    def _outputerator(output_spec: dict) -> Generator[Tuple[str, dict], None, None]:
        for sym_name, file_spec in output_spec.items():
            expanded = g.glob(file_spec["name"], recursive=True)
            if not expanded:
                logger.warning(f"no file matching '{file_spec['name']}' found in workspace")
            for filename in expanded:
                yld = file_spec.copy()
                yld["name"] = filename
                yield sym_name, yld

    def _upload_that(self, symbolic_name: str, file_spec: dict, global_tags: dict) -> str:
        local_file = file_spec["name"]
        local_tags = file_spec["s3_tags"]
        local_size = os.path.getsize(local_file)

        s3_filename = os.path.basename(local_file)

        if "dest" in file_spec:
            dest_uri = f"{file_spec['dest']}{s3_filename}"
            bucket, key = dest_uri.split("/", 3)[2:]
        else:
            bucket = self.bucket
            key = self.qualify(s3_filename)
            dest_uri = f"s3://{bucket}/{key}"

        # https://jcoenraadts.medium.com/how-to-write-tags-when-a-file-is-uploaded-to-s3-with-boto3-and-python-690f92224e2b
        tagging_str = "&".join(f"{k}={v}" for k, v in (global_tags | local_tags).items())

        logger.info(f"starting upload: {local_file} ({local_size} bytes) -> {dest_uri}")
        session = boto3.Session()
        s3 = session.resource("s3")
        s3_obj = s3.Object(bucket, key)
        s3_obj.upload_file(local_file,
                           ExtraArgs={"ServerSideEncryption": "AES256",
                                      "Metadata": _file_metadata(),
                                      "Tagging": tagging_str}
                           )
        s3_size = s3_obj.content_length

        logger.info(f"finished upload: {local_file} ({local_size} bytes) -> {dest_uri} ({s3_size} bytes)")
        return dest_uri

    def upload_outputs(self, output_spec: Dict[str, dict], global_tags: dict) -> None:
        uploader = lambda sn, fs: self._upload_that(sn, fs, global_tags)

        with ThreadPoolExecutor(max_workers=256) as executor:
            result = list(executor.map(uploader, *zip(*self._outputerator(output_spec))))  # kudos to copilot
        logger.info(f"{len(result)} files uploaded")

    def check_for_previous_run(self) -> None:
        """
        Raises SkipExecution if this step has been run before
        """
        logger.info("checking for previous run`")
        try:
            result = self._s3_file_exists(self.qualify(self.run_status_obj))
        except Exception:
            logger.warning("unable to query previous run status, assuming none")
        else:
            if result:
                raise SkipExecution("found previous run; skipping")

        logger.info("no previous run found; continuing")

    def clear_run_status(self) -> None:
        try:
            s3 = boto3.resource("s3")
            status_obj = s3.Object(self.bucket, self.qualify(self.run_status_obj))
            status_obj.delete()
        except Exception:
            logger.warning("unable to clear previous run status")

    def put_run_status(self) -> None:
        try:
            s3 = boto3.resource("s3")
            status_obj = s3.Object(self.bucket, self.qualify(self.run_status_obj))
            status_obj.put(Body=b"",
                           Metadata=_file_metadata(),
                           Tagging="bclaw.system=true")
        except Exception:
            logger.warning("failed to upload run status")
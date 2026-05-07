from concurrent.futures import ThreadPoolExecutor
# from contextlib import contextmanager
# import json
import logging
import os
from pathlib import Path
import re
# import shutil
# from tempfile import mkdtemp, NamedTemporaryFile
from typing import Generator, Tuple

import boto3
import botocore.exceptions

# from .dind import run_child_container

logger = logging.getLogger(__name__)

HOST_PARENT = Path("/mnt/s3files")
RUNNER_PARENT = Path("/_bclaw_scratch")
CHILD_PATH = Path("/_work_")

class Workspace:
    # A Workspace represents a directory on an S3Files filesystem where the job will do its work
    # and store its outputs

    def __init__(self, path: str, import_specs: list):
        # This is the location of the working directory on the S3Files filesystem
        self.raw_path = Path(path)
        logger.info(f"raw_path: {self.raw_path}")

        # This is where the working directory can be found on the host
        self.host_path = HOST_PARENT / self.raw_path.relative_to(self.raw_path.anchor)
        logger.info(f"host_path: {self.host_path}")

        # This is where the working directory can be found in bclaw_runner's Docker container
        self.runner_path = RUNNER_PARENT / self.raw_path.relative_to(self.raw_path.anchor)
        logger.info(f"runner_path: {self.runner_path}")

        # This is where the working directory will be mounted in the child Docker container
        self.child_path = CHILD_PATH
        logger.info(f"child_path: {self.child_path}")

        self.imports = import_specs

    def __enter__(self):
        logger.info(f"creating workspace: {self.runner_path}")
        # Path.mkdir(parents=True, exist_ok=True) is generally considered to be safe from race conditions if
        # concurrent jobs are running
        self.runner_path.mkdir(parents=True, exist_ok=True)
        self.do_imports()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _import_this(self, import_spec: str) -> str:
        # todo: use s3 filename if local path is missing
        s3_uri, local_path = re.split(r"\s+->\s+", import_spec, maxsplit=1)
        bucket, key = s3_uri.split("/", 3)[2:]

        dest = self.runner_path / local_path.lstrip("/")
        dest.parent.mkdir(parents=True, exist_ok=True)

        session = boto3.Session()
        s3 = session.resource("s3")
        try:
            s3_obj = s3.Object(bucket, key)
            s3_size = s3_obj.content_length
            logger.info(f"starting download: {s3_uri} ({s3_size} bytes) -> {local_path}")
            s3_obj.download_file(str(dest))
            # local_size = os.path.getsize(dest)
            local_size = dest.stat().st_size
            logger.info(f"finished download: {s3_uri} ({s3_size} bytes) -> {local_path} ({local_size} bytes)")
            return str(dest)
        except botocore.exceptions.ClientError as ce:
            if "Not Found" in str(ce):
                raise FileNotFoundError(s3_uri)
            else:
                raise

    def do_imports(self) -> None:
        with ThreadPoolExecutor(max_workers=256) as executor:
            result = list(executor.map(lambda s: self._import_this(s), self.imports))

        logger.info(f"{len(result)} files downloaded")

    def _exporterator(self, output_specs: list[str]) -> Generator[Tuple[str, str], None, None]:
        for spec in output_specs:
            src, dest = spec.split(" -> ")
            src_path = Path(src)

            if src_path.is_absolute():
                expanded = Path("/").glob(str(src_path.relative_to(src_path.anchor)))
            else:
                expanded = list(self.runner_path.glob(src))

            if not expanded:
                logger.warning(f"no file matching '{src}' found in workspace")
            for filename in expanded:
                yield str(filename), dest

    @staticmethod
    def _export_that(src: str, dst: str) -> str:
        local_size = os.path.getsize(src)

        if dst.endswith("/"):
            dst += os.path.basename(src)
        bucket, key = dst.split("/", 3)[2:]

        logger.info(f"uploading {src} to s3://{bucket}/{key}")
        logger.info(f"starting upload: {src} ({local_size} bytes) -> {dst}")

        session = boto3.Session()
        s3 = session.resource("s3")
        s3_obj = s3.Object(bucket, key)
        s3_obj.upload(src, ExtraArgs={"ServerSideEncryption": "AES256"})
        s3_size = s3_obj.content_length

        logger.info(f"finished upload: {src} ({local_size} bytes) -> {dst} ({s3_size} bytes)")

        return dst

    def do_exports(self, output_specs: list[str]) -> None:
        exporter = lambda src_dst: self._export_that(*src_dst)

        with ThreadPoolExecutor(max_workers=10) as executor:
            result = list(executor.map(exporter, self._exporterator(output_specs)))
        logger.info(f"{len(result)} files uploaded")


# class UserCommandsFailed(Exception):
#     def __init__(self, message: str, exit_code:int):
#         super().__init__(message)
#         self.exit_code = exit_code


# @contextmanager
# def workspace() -> Generator[str, None, None]:
#     orig_path = os.getcwd()
#     work_path = mkdtemp(dir=os.environ["BC_SCRATCH_PATH"])
#
#     logger.debug(f"workspace={work_path}")
#
#     try:
#         os.chdir(work_path)
#         yield work_path
#
#     finally:
#         logger.debug("cleaning up workspace")
#         os.chdir(orig_path)
#         shutil.rmtree(work_path, ignore_errors=True)
#         logger.debug("cleanup finished")


# def write_job_data_file(job_data: dict, dest_dir: str) -> str:
#     with NamedTemporaryFile(prefix="job_data_", suffix=".json", dir=dest_dir, mode="w", delete=False) as fp:
#         json.dump(job_data, fp)
#     return fp.name


# def run_commands(image_spec: dict, commands: list, work_dir: str, job_data_file: str, shell_opt: str) -> None:
#     script_file = "_commands.sh"
#
#     with open(script_file, "w") as fp:
#         for command in commands:
#             print(command, file=fp)
#
#     logger.info(f"shell option={shell_opt}")
#
#     if shell_opt == "sh":
#         shell_cmd = "sh -veu"
#     elif shell_opt == "bash":
#         shell_cmd = "bash -veuo pipefail"
#     elif shell_opt == "sh-pipefail":
#         shell_cmd = "sh -veuo pipefail"
#     else:
#         raise RuntimeError(f"unrecognized shell: {shell_opt}")
#
#     os.chmod(script_file, 0o700)
#     command = f"{shell_cmd} {script_file}"
#
#     if (exit_code := run_child_container(image_spec, command, work_dir, job_data_file)) == 0:
#         logger.info("command block succeeded")
#     else:
#         logger.error("command block failed")
#         raise UserCommandsFailed(f"command block failed with exit code {exit_code}", exit_code)

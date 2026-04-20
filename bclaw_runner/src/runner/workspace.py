# from contextlib import contextmanager
# import json
import logging
# import os
from pathlib import Path
# import shutil
# from tempfile import mkdtemp, NamedTemporaryFile
# from typing import Generator

# from .dind import run_child_container

logger = logging.getLogger(__name__)


class Workspace:
    # A Workspace represents a directory on an S3Files filesystem where the job will do its work
    # and store its outputs

    def __init__(self, path: str):
        # This is the location of the working directory on the S3Files filesystem
        self.raw_path = Path(path)

        # This is where the working directory can be found on the host
        self.host_path = Path("/mnt/s3files") / self.raw_path.relative_to(self.raw_path.anchor)

        # This is where the working directory can be found in bclaw_runner's Docker container
        self.runner_path = Path("/scratch") / self.raw_path.relative_to(self.raw_path.anchor)

        # This is where the working directory will be mounted in the child Docker container
        self.child_path = Path("/_work_")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


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

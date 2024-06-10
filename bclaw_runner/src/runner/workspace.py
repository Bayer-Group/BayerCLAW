from contextlib import contextmanager
import json
import logging
import os
import shutil
from tempfile import mkdtemp, NamedTemporaryFile

from .dind import run_child_container

logger = logging.getLogger(__name__)


class UserCommandsFailed(Exception):
    def __init__(self, message, exit_code):
        super().__init__(message)
        self.exit_code = exit_code


@contextmanager
def workspace() -> str:
    orig_path = os.getcwd()
    work_path = mkdtemp(dir=os.environ["BC_SCRATCH_PATH"])

    logger.debug(f"workspace: {work_path}")

    try:
        os.chdir(work_path)
        yield work_path

    finally:
        logger.info("cleaning up workspace")
        os.chdir(orig_path)
        shutil.rmtree(work_path, ignore_errors=True)
        logger.info("finished")


def write_job_data_file(job_data: dict, dest_dir: str) -> str:
    with NamedTemporaryFile(prefix="job_data_", suffix=".json", dir=dest_dir, mode="w", delete=False) as fp:
        json.dump(job_data, fp)
    return fp.name


def run_commands(image_tag: str, commands: list, work_dir: str, job_data_file: str, shell_opt: str) -> None:
    script_file = "_commands.sh"

    with open(script_file, "w") as fp:
        for command in commands:
            print(command, file=fp)

    if shell_opt == "sh":
        shell_cmd = "sh -veu"
    elif shell_opt == "bash":
        shell_cmd = "bash -veuo pipefail"
    elif shell_opt == "sh-pipefail":
        shell_cmd = "sh -veuo pipefail"
    else:
        raise RuntimeError(f"unrecognized shell: {shell_opt}")

    os.chmod(script_file, 0o700)
    command = f"{shell_cmd} {script_file}"

    if (exit_code := run_child_container(image_tag, command, work_dir, job_data_file)) == 0:
        logger.info("command block succeeded")
    else:
        raise UserCommandsFailed(f"command block failed with exit code {exit_code}", exit_code)

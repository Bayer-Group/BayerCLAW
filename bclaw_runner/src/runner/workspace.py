from contextlib import contextmanager
import json
import logging
import os
import shutil
from tempfile import mkdtemp, NamedTemporaryFile

from .dind import run_child_container

logger = logging.getLogger(__name__)


@contextmanager
def workspace() -> str:
    orig_path = os.getcwd()
    work_path = mkdtemp(dir=os.environ["BC_SCRATCH_PATH"])

    logger.debug(f"workspace: {work_path}")

    try:
        os.chdir(work_path)
        yield work_path

    finally:
        os.chdir(orig_path)
        shutil.rmtree(work_path, ignore_errors=True)


def write_job_data_file(job_data: dict, dest_dir: str) -> str:
    with NamedTemporaryFile(prefix="job_data_", suffix=".json", dir=dest_dir, mode="w", delete=False) as fp:
        json.dump(job_data, fp)
    return fp.name


def run_commands(image_tag: str, commands: list, work_dir: str, job_data_file: str) -> int:
    script_file = "_commands.sh"

    with open(script_file, "w") as fp:
        for command in commands:
            print(command, file=fp)

    os.chmod(script_file, 0o700)
    command = f"sh -veu {script_file}"

    ret = run_child_container(image_tag, command, work_dir, job_data_file)

    return ret

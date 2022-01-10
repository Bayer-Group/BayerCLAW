from contextlib import contextmanager
import json
import logging
import os
import shutil
from tempfile import mkdtemp, NamedTemporaryFile

from .dind import run_child_container
from .runnit import runnit

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


def run_commands1(image_tag: str, commands: list, work_dir: str, job_data_file: str) -> int:
    script_file = "_commands.sh"

    with open(script_file, "w") as fp:
        for command in commands:
            print(command, file=fp)

    os.chmod(script_file, 0o700)
    command = f"sh -veu {script_file}"

    ret = run_child_container(image_tag, command, work_dir, job_data_file)

    return ret


def run_commands(commands: list, cfg: dict, work_dir: str, job_data_file: str) -> int:
    script_file = "_commands.sh"

    with open(script_file, "w") as fp:
        print(cfg["shell-opts"], file=fp)  # todo: set -veu
        for command in commands:
            print(command, file=fp)

    os.chmod(script_file, 0o700)

    # todo: make command string

    # todo: remove
    # https://pyinstaller.readthedocs.io/en/stable/runtime-information.html#ld-library-path-libpath-considerations
    env = dict(os.environ)
    lp_key = "LD_LIBRARY_PATH"
    lp_orig = env.get(lp_key + "_ORIG")
    if lp_orig is not None:
        env[lp_key] = lp_orig
    else:
        # this happens when LD_LIBRARY_PATH was not set
        # remove the env var set by pyinstaller
        env.pop(lp_key, None)

    # todo: remove, pass job_data_file to runner
    # todo: fixed name for job data file?
    env["BC_WORKSPACE"] = work_dir
    env["BC_JOB_DATA_FILE"] = job_data_file

    # todo: get image name from somewhere

    # todo: run_child_container
    ret = runnit([cfg["shell-exe"], script_file], env=env)

    return ret

"""
run stuff

Usage:
    bclaw_runner.py [options]

Options:
    --cmd COMMAND        command
    --in JSON_STRING     input files
    --out JSON_STRING    output files
    --param JSON_STRING  parameter substitution [default: {}]
    --ref JSON_STRING    reference files
    --repo S3_PATH       repository path
    --skip STRING        step skip condition: output, rerun, none [default: none]
    --help -h            show help
    --version            show version
"""

import json
import logging.config
import os
import sys
from typing import Dict, List, Tuple

from docopt import docopt
from more_itertools import partition

from .cache import get_reference_inputs
from .custom_logs import LOGGING_CONFIG
from .string_subs import substitute
from .repo import Repository
from .termination import spot_termination_checker
from .version import VERSION
from .workspace import workspace, write_job_data_file, run_commands


logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def get_config() -> dict:
    # figure out where this code lives. Config file location will be relative to this directory.
    #   see https://pyinstaller.readthedocs.io/en/stable/runtime-information.html#using-file
    home_dir = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))

    # check whether this is running from a PyInstaller bundle or as a normal Python script
    #   see https://pyinstaller.readthedocs.io/en/stable/runtime-information.html
    if hasattr(sys, "frozen") and hasattr(sys, "_MEIPASS"):
        logger.debug("running in a PyInstaller bundle")

        # PyInstaller build command should be set up to put the appropriate cfg.json in the same
        # directory as this file
        cfg_path = os.path.join(home_dir, "cfg.json")

    else:
        logger.debug("running in a normal Python process")

        # default to using the bash config file. Use the CONFIG environment variable to override
        cfg_path = os.path.join(home_dir, "cfg", os.environ.get("CONFIG", "bash"), "cfg.json")

    with open(cfg_path) as fp:
        cfg = json.load(fp)

    return cfg


# this emulates operator.itemgetter, except that it always returns a tuple
def my_itemgetter(*items):
    def _impl(obj):
        return tuple(obj[i] for i in items)
    return _impl


def split_inputs(all_inputs: dict) -> Tuple[Dict, Dict]:
    required_iter, optional_iter = partition(lambda s: s.endswith("?"), all_inputs)

    required_keys = list(required_iter)
    try:
        required_getter = my_itemgetter(*required_keys)
        required_ret = {k: v for k, v in zip(required_keys, required_getter(all_inputs))}
    except TypeError:
        required_ret = {}

    optional_keys = list(optional_iter)
    try:
        optional_getter = my_itemgetter(*optional_keys)
        optional_ret = {k.rstrip("?"): v for k, v in zip(optional_keys, optional_getter(all_inputs))}
    except TypeError:
        optional_ret = {}

    return required_ret, optional_ret


def main(commands: List[str],
         inputs: Dict[str, str],
         outputs: Dict[str, str],
         params: Dict[str, str],
         references: Dict[str, str],
         repo_path: str,
         skip: str) -> int:
    shell_config = get_config()

    repo = Repository(repo_path)

    if skip == "rerun":
        if repo.check_for_previous_run():
            logger.info("found previous run; skipping")
            return 0
    elif skip == "output":
        if repo.files_exist(list(outputs.values())):
            logger.info("found output files; skipping")
            return 0

    repo.clear_run_status()

    job_data_obj = repo.read_job_data()

    jobby_commands   = substitute(commands,   job_data_obj)
    jobby_inputs     = substitute(inputs,     job_data_obj)
    jobby_outputs    = substitute(outputs,    job_data_obj)
    jobby_params     = substitute(params,     job_data_obj)
    jobby_references = substitute(references, job_data_obj)

    subbed_inputs  = substitute(jobby_inputs,  jobby_params)
    subbed_outputs = substitute(jobby_outputs, jobby_params)
    # params will not be substituted into reference values

    with workspace() as wrk:
        try:
            # download references, link to workspace
            local_references = get_reference_inputs(jobby_references)

            # split inputs into required & optional
            required_inputs, optional_inputs = split_inputs(subbed_inputs)

            # download inputs -> returns local filenames
            local_required_inputs = repo.download_inputs(required_inputs, optional=False)
            local_optional_inputs = repo.download_inputs(optional_inputs, optional=True)
            local_outputs = subbed_outputs

            # substitute local filenames into commands
            subbed_commands = substitute(jobby_commands, {**local_required_inputs,
                                                          **local_optional_inputs,
                                                          **local_outputs,
                                                          **jobby_params,
                                                          **local_references})

            local_job_data = write_job_data_file(job_data_obj, wrk)

            # run commands
            status = run_commands(subbed_commands, shell_config, wrk, local_job_data)
            if status == 0:
                logger.info("command block succeeded")
            else:
                logger.error(f"command block failed with exit code {status}")

            # upload outputs
            repo.upload_outputs(subbed_outputs)

            # mark job complete on success
            if status == 0:
                try:
                    repo.put_run_status()
                except RuntimeError:
                    logger.warning("failed to upload run status")

        except Exception as e:
            logger.exception("runner failed")
            status = 255

        else:
            logger.info("runner finished")
    return status


def cli() -> int:
    with spot_termination_checker():
        args = docopt(__doc__, version=VERSION)

        logger.info(json.dumps(args, indent=4))

        commands = json.loads(args["--cmd"])
        inputs   = json.loads(args["--in"])
        outputs  = json.loads(args["--out"])
        params   = json.loads(args["--param"])
        refs     = json.loads(args["--ref"])
        repo     = args["--repo"]
        skip     = args["--skip"]

        ret = main(commands, inputs, outputs, params, refs, repo, skip)
        return ret

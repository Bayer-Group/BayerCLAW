"""
run stuff

Usage:
    bclaw_runner.py [options]

Options:
    --cmd COMMAND        command
    --image STRING       Docker image tag
    --in JSON_STRING     input files
    --out JSON_STRING    output files
    --ref JSON_STRING    reference files
    --repo S3_PATH       repository path
    --shell SHELL        unix shell to run commands in (bash | sh | sh-pipefail) [default: sh]
    --skip STRING        step skip condition: output, rerun, none [default: none]
    --help -h            show help
    --version            show version
"""

import json
import logging.config
from typing import Dict, List

from docopt import docopt

from .cache import get_reference_inputs
from .custom_logs import LOGGING_CONFIG
from .string_subs import substitute, substitute_image_tag
from .repo import Repository
from .termination import spot_termination_checker
from .version import VERSION
from .workspace import workspace, write_job_data_file, run_commands


logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def main(commands: List[str],
         image: str,
         inputs: Dict[str, str],
         outputs: Dict[str, str],
         references: Dict[str, str],
         repo_path: str,
         shell: str,
         skip: str) -> int:

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
    jobby_references = substitute(references, job_data_obj)

    jobby_image = substitute_image_tag(image, job_data_obj)

    with workspace() as wrk:
        try:
            # download references, link to workspace
            local_references = get_reference_inputs(jobby_references)

            # download inputs -> returns local filenames
            local_inputs = repo.download_inputs(jobby_inputs)
            local_outputs = jobby_outputs

            subbed_commands = substitute(jobby_commands,
                                         local_inputs |
                                         local_outputs |
                                         local_references)

            local_job_data = write_job_data_file(job_data_obj, wrk)

            # run commands
            status = run_commands(jobby_image, subbed_commands, wrk, local_job_data, shell)
            if status == 0:
                logger.info("command block succeeded")
            else:
                logger.error(f"command block failed with exit code {status}")

            # upload outputs
            repo.upload_outputs(jobby_outputs)

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
        image    = args["--image"]
        inputs   = json.loads(args["--in"])
        outputs  = json.loads(args["--out"])
        refs     = json.loads(args["--ref"])
        repo     = args["--repo"]
        shell    = args["--shell"]
        skip     = args["--skip"]

        ret = main(commands, image, inputs, outputs, refs, repo, shell, skip)
        return ret

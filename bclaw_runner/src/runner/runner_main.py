"""
run stuff

Usage:
    bclaw_runner.py [options]

Options:
    -c COMMANDS     command
    -f JSON_STRING  reference files
    -i JSON_STRING  input files
    -k STRING       step skip condition: output, rerun, none [default: none]
    -m STRING       Docker image tag
    -o JSON_STRING  output files
    -q JSON_STRING  QC check spec
    -r S3_PATH      repository path
    -s SHELL        unix shell to run commands in (bash | sh | sh-pipefail) [default: sh]
    -t JSON_STRING  global s3 tags
    -h              show help
    --version       show version
"""

from functools import partial, partialmethod
import json
import logging.config
import os
from typing import Dict, List

from docopt import docopt

from .cache import get_reference_inputs
from .custom_logs import LOGGING_CONFIG
from .string_subs import substitute, substitute_image_tag
from .qc_check import do_checks, abort_execution, QCFailure
from .repo import Repository, SkipExecution
from .tagging import tag_this_instance
from .termination import spot_termination_checker
from .workspace import workspace, write_job_data_file, run_commands, UserCommandsFailed

# logging.config.dictConfig(LOGGING_CONFIG)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(commands: List[str],
         image: str,
         inputs: Dict[str, str],
         outputs: Dict[str, str | Dict],
         qc: List[dict],
         references: Dict[str, str],
         repo_path: str,
         shell: str,
         skip: str,
         tags: Dict[str, str]) -> int:
    exit_code = 0
    try:
        repo = Repository(repo_path)

        if skip == "rerun":
            repo.check_for_previous_run()
        elif skip == "output":
            repo.check_files_exist(list(outputs.values()))

        repo.clear_run_status()

        job_data_obj = repo.read_job_data()

        jobby_commands   = substitute(commands,   job_data_obj)
        jobby_inputs     = substitute(inputs,     job_data_obj)
        jobby_outputs    = substitute(outputs,    job_data_obj)  # this will recurse down to s3_tags
        jobby_references = substitute(references, job_data_obj)
        jobby_tags       = substitute(tags,       job_data_obj)

        jobby_image = substitute_image_tag(image, job_data_obj)

        with workspace() as wrk:
            # download references, link to workspace
            local_references = get_reference_inputs(jobby_references)

            # download inputs -> returns local filenames
            local_inputs = repo.download_inputs(jobby_inputs)
            local_outputs = {k.rstrip("!"): v["name"] for k, v in jobby_outputs.items()}

            subbed_commands = substitute(jobby_commands,
                                         local_inputs |
                                         local_outputs |
                                         local_references)

            local_job_data = write_job_data_file(job_data_obj, wrk)

            try:
                run_commands(jobby_image, subbed_commands, wrk, local_job_data, shell)
                do_checks(qc)

            finally:
                repo.upload_outputs1(jobby_outputs, jobby_tags)

    except UserCommandsFailed as uce:
        logger.error(str(uce))
        exit_code = uce.exit_code

    except QCFailure as qcf:
        logger.error(str(qcf))
        abort_execution(qcf.failures)

    except SkipExecution as se:
        logger.info(str(se))
        pass

    except Exception as e:
        logger.exception("bclaw_runner error: ")
        exit_code = 199

    else:
        repo.put_run_status()
        logger.info("runner finished")

    return exit_code


def cli() -> int:
    tag_this_instance()

    # create custom log level for user commands
    # https://stackoverflow.com/a/55276759
    logging.USER_CMD = logging.INFO + 5  # between INFO and WARNING
    logging.addLevelName(logging.USER_CMD, "USER_CMD")
    logging.Logger.user_cmd = partialmethod(logging.Logger.log, logging.USER_CMD)
    logging.user_cmd = partial(logging.log, logging.USER_CMD)

    with spot_termination_checker():
        args = docopt(__doc__, version=os.environ["BC_VERSION"])

        logger.info(f"{args=}")

        commands = json.loads(args["-c"])
        image    = args["-m"]
        inputs   = json.loads(args["-i"])
        outputs  = json.loads(args["-o"])
        qc       = json.loads(args["-q"])
        refs     = json.loads(args["-f"])
        repo     = args["-r"]
        shell    = args["-s"]
        skip     = args["-k"]
        tags     = json.loads(args["-t"])

        ret = main(commands, image, inputs, outputs, qc, refs, repo, shell, skip, tags)
        return ret

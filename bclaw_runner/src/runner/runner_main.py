"""
run stuff

Usage:
    bclaw_runner.py [options]

Options:
    -c COMMANDS     command
    -f JSON_STRING  reference files
    -i JSON_STRING  input files
    -k STRING       step skip condition: output, rerun, none [default: none]
    -m JSON_STRING  Docker image spec
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

# from .cache import get_reference_inputs
from .dind import run_child_container
from .string_subs import substitute, substitute_image_tag
from .preamble import log_preamble
# from .qc_check import do_checks, abort_execution, QCFailure
# from .repo import Repository, SkipExecution
from .instance import get_imdsv2_token, tag_this_instance, spot_termination_checker
# from .workspace import workspace, write_job_data_file, run_commands, UserCommandsFailed
from .workspace import Workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserCommandsFailed(Exception):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code


# def main0(commands: List[str],
#          image_spec: dict,
#          inputs: Dict[str, str],
#          outputs: Dict[str, str | Dict],
#          qc: List[dict],
#          references: Dict[str, str],
#          repo_path: str,
#          shell: str,
#          skip: str,
#          tags: Dict[str, str]) -> int:
#     exit_code = 0
#     try:
#         repo = Repository(repo_path)
#
#         if skip == "rerun":
#             repo.check_for_previous_run()
#         elif skip == "output":
#             repo.check_files_exist(list(outputs.values()))
#
#         repo.clear_run_status()
#
#         job_data_obj = repo.read_job_data()
#
#         jobby_commands   = substitute(commands,   job_data_obj)
#         jobby_inputs     = substitute(inputs,     job_data_obj)
#         jobby_outputs    = substitute(outputs,    job_data_obj)  # this will recurse down to s3_tags
#         jobby_references = substitute(references, job_data_obj)
#         jobby_tags       = substitute(tags,       job_data_obj)
#
#         jobby_image_spec = substitute_image_tag(image_spec, job_data_obj)
#
#         with workspace() as wrk:
#             # download references, link to workspace
#             local_references = get_reference_inputs(jobby_references)
#
#             # download inputs -> returns local filenames
#             local_inputs = repo.download_inputs(jobby_inputs)
#             local_outputs = {k.rstrip("!"): v["name"] for k, v in jobby_outputs.items()}
#
#             subbed_commands = substitute(jobby_commands,
#                                          local_inputs |
#                                          local_outputs |
#                                          local_references)
#
#             local_job_data = write_job_data_file(job_data_obj, wrk)
#
#             try:
#                 run_commands(jobby_image_spec, subbed_commands, wrk, local_job_data, shell)
#                 do_checks(qc)
#
#             finally:
#                 repo.upload_outputs(jobby_outputs, jobby_tags)
#
#     except UserCommandsFailed as uce:
#         logger.error(str(uce))
#         exit_code = uce.exit_code
#
#     except QCFailure as qcf:
#         logger.error(str(qcf))
#         abort_execution(qcf.failures)
#
#     except SkipExecution as se:
#         logger.info(str(se))
#         pass
#
#     except Exception as e:
#         logger.exception("bclaw_runner error: ")
#         exit_code = 199
#
#     else:
#         repo.put_run_status()
#         logger.info("runner finished")
#
#     return exit_code
#


def command_runner(commands: List[str], image_spec: dict, repo: str, shell: str) -> None:
    if shell == "sh":
        shell_cmd = "sh -veu"
    elif shell == "bash":
        shell_cmd = "bash -veuo pipefail"
    elif shell == "sh-pipefail":
        shell_cmd = "sh -veuo pipefail"
    else:
        raise RuntimeError(f"unrecognized shell: {shell}")

    with Workspace(repo) as workspace:
        runner_script_file = workspace.runner_path / "_commands.sh"
        with runner_script_file.open("w") as fp:
            for cmd_line in commands:
                print(cmd_line, file=fp)

        child_script_file = workspace.child_runner_path / runner_script_file.name
        command = f"{shell_cmd} {child_script_file}"

        if (exit_code := run_child_container(image_spec, command, workspace)) == 0:
            logger.info("command block succeeded")
        else:
            logger.error("command block failed")
            raise UserCommandsFailed(f"command block failed with exit code {exit_code}", exit_code)


def main(commands: List[str], image_spec: dict, repo: str, shell: str) -> int:
    exit_code = 0

    try:
        command_runner(commands, image_spec, repo, shell)

    except UserCommandsFailed as ucf:
        logger.error(str(ucf))
        exit_code = ucf.exit_code

    except Exception as e:
        logger.exception("bclaw_runner error: ")
        exit_code = 199

    else:
        logger.info("bclaw_runner finished")

    return exit_code


def cli() -> int:
    args = docopt(__doc__, version=os.environ["BC_VERSION"])

    log_preamble()
    logger.info("---------- bclaw_runner starting ----------")
    get_imdsv2_token()
    tag_this_instance()

    # create custom log level for user commands
    # https://stackoverflow.com/a/55276759
    logging.USER_CMD = logging.INFO + 5  # between INFO and WARNING
    logging.addLevelName(logging.USER_CMD, "USER_CMD")
    logging.Logger.user_cmd = partialmethod(logging.Logger.log, logging.USER_CMD)
    logging.user_cmd = partial(logging.log, logging.USER_CMD)

    with spot_termination_checker():
        commands = json.loads(args["-c"])
        image    = json.loads(args["-m"])
        # inputs   = json.loads(args["-i"])
        # outputs  = json.loads(args["-o"])
        # qc       = json.loads(args["-q"])
        # refs     = json.loads(args["-f"])
        repo     = args["-r"]
        shell    = args["-s"]
        # skip     = args["-k"]
        # tags     = json.loads(args["-t"])

        ret = main(commands, image, repo, shell)
        return ret

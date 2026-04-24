"""
run stuff

Usage:
    bclaw_runner.py [options]

Options:
    -c COMMANDS     command
    -i JSON_STRING  S3 file imports
    -m JSON_STRING  Docker image spec
    -r PATH         repository path
    -s SHELL        unix shell to run commands in (bash | sh | sh-pipefail) [default: sh]
    -z TOKEN        task token
    -h              show help
    --version       show version
"""

from functools import partial, partialmethod
import json
import logging.config
import os
import time
from typing import Dict, List

import boto3
from docopt import docopt

# from .cache import get_reference_inputs
from .dind import run_child_container
from .string_subs import substitute, substitute_image_tag
from .preamble import log_preamble
# from .qc_check import do_checks, abort_execution, QCFailure
# from .repo import Repository, SkipExecution
from .inline_cmds import UserDefinedError
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
        logger.info(f"creating workspace: {workspace.runner_path}")
        # Path.mkdir(parents=True, exist_ok=True) is generally considered to be safe from race conditions if
        # concurrent jobs are running
        # todo: move into Workspace
        workspace.runner_path.mkdir(parents=True, exist_ok=True)

        # todo: download imports

        # todo: use different names so concurrent jobs can run
        # todo: clean up after run?
        runner_script_file = workspace.runner_path / "_commands.sh"
        with runner_script_file.open("w") as fp:
            for cmd_line in commands:
                print(cmd_line, file=fp)

        child_script_file = workspace.child_path / runner_script_file.name
        command = f"{shell_cmd} {child_script_file}"

        if (exit_code := run_child_container(image_spec, command, workspace)) == 0:
            logger.info("command block succeeded")
        else:
            logger.error("command block failed")
            raise UserCommandsFailed(f"command block failed with exit code {exit_code}", exit_code)


def main(commands: List[str], imports: list[str], image_spec: dict, repo: str, shell: str, token: str) -> int:
    sfn = boto3.client("stepfunctions")

    exit_code = 0

    try:
        command_runner(commands, image_spec, repo, shell)

    except UserCommandsFailed as ucf:
        logger.error(str(ucf))
        sfn.send_task_failure(
            taskToken=token,
            error="User commands failed",
            cause=str(ucf)
        )
        exit_code = ucf.exit_code

    except UserDefinedError as ude:
        logger.error(str(ude))
        sfn.send_task_failure(
            taskToken=token,
            error=ude.title,
            cause=str(ude)
        )
        exit_code = 198

    except Exception as e:
        logger.exception("bclaw_runner error: ")
        sfn.send_task_failure(
            taskToken=token,
            error=type(e).__name__,
            cause=str(e),
        )
        exit_code = 199

    else:
        sfn.send_task_success(
            taskToken=token,
            output=json.dumps({"status": "SUCCEEDED"}),
        )
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
        imports  = json.loads(args["-i"])
        image    = json.loads(args["-m"])
        repo     = args["-r"]
        shell    = args["-s"]
        token    = args["-z"]

        ret = main(commands, imports, image, repo, shell, token)
        return ret

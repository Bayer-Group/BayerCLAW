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
    -x JSON_STRING  S3 file exports
    -z TOKEN        task token
    -h              show help
    --version       show version
"""

from contextlib import closing
from functools import partial, partialmethod
import json
import logging.config
import os
import re
import tempfile
import time
from typing import Any, Dict, List

import boto3
from docopt import docopt

# from .cache import get_reference_inputs
from .dind import run_child_container
# from .string_subs import substitute, substitute_image_tag
from .preamble import log_preamble
# from .exports import do_exports
# from .qc_check import do_checks, abort_execution, QCFailure
# from .repo import Repository, SkipExecution
from .inline_cmds import StopRequested
from .instance import get_imdsv2_token, tag_this_instance, spot_termination_checker
# from .workspace import workspace, write_job_data_file, run_commands, UserCommandsFailed
from .workspace import Workspace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserCommandsFailed(Exception):
    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code


def read_jobfile() -> dict:
    bucket = os.environ["BC_LAUNCH_BUCKET"]
    key = os.environ["BC_LAUNCH_KEY"]
    version = os.environ["BC_LAUNCH_VERSION"]

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key, VersionId=version)

    with closing(response["Body"]) as fp:
        ret = json.load(fp)
    return ret


# SUB_FINDER = re.compile(r"\${{(.+?)}}")
SUB_FINDER = re.compile(r"\${job\.(.+?)}")

# todo: use jmespath to get nested fields
def substitute(target: Any, spec: dict) -> Any:
    if isinstance(target, str):
        ret = SUB_FINDER.sub(lambda m: str(spec.get(m.group(1), m.group(0))) , target)
    elif isinstance(target, list):
        ret = [substitute(v, spec) for v in target]
    elif isinstance(target, dict):
        ret = {k: substitute(v, spec) for k, v in target.items()}
    else:
        ret = target
    return ret


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


def command_runner(commands: List[str],
                   imports: List[str],
                   exports: List[str],
                   image_spec: dict,
                   repo: str,
                   shell: str) -> None:
    if shell == "sh":
        shell_cmd = "sh -veu"
    elif shell == "bash":
        shell_cmd = "bash -veuo pipefail"
    elif shell == "sh-pipefail":
        shell_cmd = "sh -veuo pipefail"
    else:
        raise RuntimeError(f"unrecognized shell: {shell}")

    job_data = read_jobfile()

    jobby_imports = substitute(imports, job_data)
    jobby_commands = substitute(commands, job_data)
    jobby_exports = substitute(exports, job_data)

    with Workspace(repo, jobby_imports) as workspace:
        with tempfile.NamedTemporaryFile(prefix="cmd_", suffix=".sh", dir=workspace.runner_path, mode="w+t") as script_file:
            for cmd_line in jobby_commands:
                print(cmd_line, file=script_file)

            script_file.flush()
            script_file.seek(0)

            # logger.info(f"local script file: {script_file.name}")

            child_script_file = workspace.child_path / os.path.basename(script_file.name)

            # logger.info(f"child script file: {child_script_file.name}")

            command = f"{shell_cmd} {child_script_file}"

            if (exit_code := run_child_container(image_spec, command, workspace)) == 0:
                logger.info("command block succeeded")
                workspace.do_exports(jobby_exports)
            else:
                logger.error("command block failed")
                raise UserCommandsFailed(f"command block failed with exit code {exit_code}", exit_code)


def main(commands: List[str], imports: list[str], exports: list[str], image_spec: dict, repo: str, shell: str, token: str) -> int:
    sfn = boto3.client("stepfunctions")

    exit_code = 0

    try:
        command_runner(commands, imports, exports, image_spec, repo, shell)

    except UserCommandsFailed as ucf:
        logger.error(str(ucf))
        # todo: in the case of a spot instance termination, step functions may terminate the task before this gets
        #   processed, thus raising a botocore.errorfactory.TaskTimedOut. Basically harmless, but it writes an ugly
        #   error message to the logs. Catch it to avoid user freak outs
        sfn.send_task_failure(
            taskToken=token,
            error="User commands failed",
            cause=str(ucf)
        )
        exit_code = ucf.exit_code

    except StopRequested as se:
        logger.error(str(se))
        sfn.send_task_failure(
            taskToken=token,
            error=se.error,
            cause=str(se)
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
        exports  = json.loads(args["-x"])
        token    = args["-z"]

        ret = main(commands, imports, exports, image, repo, shell, token)
        return ret

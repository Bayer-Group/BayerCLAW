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
    -h              show help
    --version       show version
"""

from contextlib import closing
import json
import logging.config
import os
import tempfile
from typing import List

import boto3
from docopt import docopt

from .dind import run_child_container
from .string_subs import substitute
from .preamble import log_preamble
from .inline_cmds import StopRequested
from .instance import get_imdsv2_token, tag_this_instance, spot_termination_checker
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

    # wrap the job data file contents in this dict so that substitute() will
    # handle things like "${job.foo}" properly
    job_data = {"job": read_jobfile()}

    jobby_imports = substitute(imports, job_data)
    jobby_commands = substitute(commands, job_data)
    jobby_exports = substitute(exports, job_data)

    with Workspace(repo, jobby_imports) as workspace:
        with tempfile.NamedTemporaryFile(prefix="cmd_", suffix=".sh", dir=workspace.runner_path, mode="w+t") as script_file:
            for cmd_line in jobby_commands:
                print(cmd_line, file=script_file)

            script_file.flush()
            script_file.seek(0)

            child_script_file = workspace.child_path / os.path.basename(script_file.name)

            command = f"{shell_cmd} {child_script_file}"

            if (exit_code := run_child_container(image_spec, command, workspace)) == 0:
                logger.info("command block succeeded")
                workspace.do_exports(jobby_exports)
            else:
                logger.error("command block failed")
                raise UserCommandsFailed(f"command block failed with exit code {exit_code}", exit_code)


def main(commands: List[str], imports: list[str], exports: list[str], image_spec: dict, repo: str, shell: str) -> int:
    task_token = os.environ["BC_TASK_TOKEN"]
    sfn = boto3.client("stepfunctions")

    exit_code = 0

    try:
        command_runner(commands, imports, exports, image_spec, repo, shell)

    except UserCommandsFailed as ucf:
        logger.error(str(ucf))
        exit_code = ucf.exit_code
        # In certain cases (e.g. execution stopped from tne console, spot instance termination, probably others),
        # step functions may terminate the task before this gets processed, thus raising a  TaskTimedOut exception.
        # This is basically harmless, but it writes an ugly error message to the logs so we suppress it.
        try:
            sfn.send_task_failure(
                taskToken=task_token,
                error="User commands failed",
                cause=str(ucf)
            )
        except sfn.exceptions.TaskTimedOut:
            pass

    except StopRequested as se:
        logger.error(str(se))
        exit_code = 198
        try:
            sfn.send_task_failure(
                taskToken=task_token,
                error=se.error,
                cause=str(se)
            )
        except sfn.exceptions.TaskTimedOut:
            pass

    except Exception as e:
        logger.exception("bclaw_runner error: ")
        exit_code = 199
        try:
            sfn.send_task_failure(
                taskToken=task_token,
                error=type(e).__name__,
                cause=str(e),
            )
        except sfn.exceptions.TaskTimedOut:
            pass

    else:
        # The Step Functions - Batch service integration does not formally support the ".waitForTaskToken" pattern.
        # All this means is that the Batch SubmitJob API does not define a specific field to pass in a task token.
        # However, a Step Functions task token can be passed to a batch job as an environment variable, so we take
        # advantage of that. This approach enables the runner to emit user-defined error types for branch-on-error
        # to work with, and to pass information from the Batch job to the state machine (although the latter is not
        # used yet).
        sfn.send_task_success(
            taskToken=task_token,
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
    # logging.USER_CMD = logging.INFO + 5  # between INFO and WARNING
    # logging.addLevelName(logging.USER_CMD, "USER_CMD")
    # logging.Logger.user_cmd = partialmethod(logging.Logger.log, logging.USER_CMD)
    # logging.user_cmd = partial(logging.log, logging.USER_CMD)

    with spot_termination_checker():
        commands = json.loads(args["-c"])
        imports  = json.loads(args["-i"])
        image    = json.loads(args["-m"])
        repo     = args["-r"]
        shell    = args["-s"]
        exports  = json.loads(args["-x"])

        ret = main(commands, imports, exports, image, repo, shell)
        return ret

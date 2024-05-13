from collections import deque
import json
import logging
import os
from typing import Generator, List, Dict
from uuid import uuid4

import boto3

from . import batch_resources as b
from . import chooser_resources as c
from . import enhanced_parallel_resources as ep
from . import native_step_resources as ns
from . import scatter_gather_resources as sg
from . import subpipe_resources as sp
from .util import Step, Resource, State, make_logical_name, lambda_logging_block, lambda_retry
from .validation import validate_batch_step, validate_native_step, validate_parallel_step, validate_scatter_step, \
    validate_subpipe_step, validate_chooser_step

STATE_MACHINE_VERSION_NAME = "mainStateMachineVersion"
STATE_MACHINE_ALIAS_NAME = "mainStateMachineAlias"


def make_initializer_step(repository: str) -> dict:
    initialize_step_name = "Initialize"

    ret = {
        initialize_step_name: {
            "Type": "Task",
            "Resource": os.environ["INITIALIZER_LAMBDA_ARN"],
            "Parameters": {
                "repo_template": repository,
                "input_obj.$": "$",
                **lambda_logging_block(initialize_step_name),
            },
            **lambda_retry(),
            "ResultPath": "$",
            "OutputPath": "$",
            "_stet": True,
        },
    }

    return ret


def make_step_list(steps: List[Dict]) -> List[Step]:
    ret = deque()
    next_step = ""

    for step in reversed(steps):
        name, spec = next(iter(step.items()))
        n = spec.get("Next") or spec.get("next")
        e = spec.get("End") or spec.get("end")

        if n is None and e is None:
            step = Step(name, spec, next_step)
        elif n is not None:
            step = Step(name, spec, n)
        else:
            step = Step(name, spec, "")

        ret.appendleft(step)
        next_step = name

    return list(ret)


def process_step(step: Step,
                 options: dict,
                 depth: int) -> Generator[Resource, None, List[State]]:
    if "scatter" in step.spec:
        normalized_step = validate_scatter_step(step)
        states_to_add = yield from sg.handle_scatter_gather(normalized_step,
                                                            options,
                                                            depth)

    elif "image" in step.spec:
        normalized_step = validate_batch_step(step)
        scattered = depth > 0
        states_to_add = yield from b.handle_batch(normalized_step,
                                                  options,
                                                  scattered)

    elif "subpipe" in step.spec:
        normalized_step = validate_subpipe_step(step)
        states_to_add = sp.handle_subpipe(normalized_step)

    elif "Type" in step.spec:
        normalized_step = validate_native_step(step)
        states_to_add = yield from ns.handle_native_step(normalized_step,
                                                         options,
                                                         depth)

    elif "choices" in step.spec:
        normalized_step = validate_chooser_step(step)
        states_to_add = c.handle_chooser_step(normalized_step)

    elif "branches" in step.spec:
        normalized_step = validate_parallel_step(step)
        states_to_add = yield from ep.handle_parallel_step(normalized_step,
                                                           options,
                                                           depth)

    else:
        raise RuntimeError(f"step '{step.name}' is not a recognized step type")

    return states_to_add


def make_branch(raw_steps: list,
                options: dict,
                repository: str = None,
                depth: int = 0) -> Generator[Resource, None, dict]:
    logger = logging.getLogger(__name__)

    if repository is not None:
        initializer_step = make_initializer_step(repository)
        raw_steps.insert(0, initializer_step)

    steps = make_step_list(raw_steps)
    states = {}

    for step in steps:
        states_to_add = yield from process_step(step, options, depth)
        states.update(states_to_add)

    ret = {
        "StartAt": steps[0].name,
        "States": states,
    }

    return ret


def write_state_machine_to_fh(sfn_def: dict, fh) -> dict:
    json.dump(sfn_def, fh, indent=4)

    ret = {
        "Bucket": "FILE",
        "Key": "FILE",
        "Version": "FILE",
    }

    return ret


def write_state_machine_to_s3(sfn_def: dict) -> dict:
    def_json = json.dumps(sfn_def, indent=4)

    # bucket = os.environ["RESOURCE_BUCKET_NAME"]
    bucket = os.environ["LAUNCHER_BUCKET_NAME"]

    base_filename = uuid4().hex
    key = f"__tmp__/stepfunctions/{base_filename}.json"

    s3 = boto3.client("s3")
    response = s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=def_json.encode("utf-8")
    )

    ret = {
        "Bucket": bucket,
        "Key": key,
    }

    if "VersionId" in response:
        ret["Version"] = response["VersionId"]

    return ret


def handle_state_machine(raw_steps: List[Dict],
                         options: dict,
                         repository: str,
                         dst_fh=None) -> Generator[Resource, None, str]:
    state_machine_def = yield from make_branch(raw_steps, options, repository=repository)

    if dst_fh is None:
        state_machine_location = write_state_machine_to_s3(state_machine_def)
    else:
        state_machine_location = write_state_machine_to_fh(state_machine_def, dst_fh)

    ret = {
        "Type": "AWS::StepFunctions::StateMachine",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "StateMachineName": {"Ref": "AWS::StackName"},
            "RoleArn": os.environ["STATES_EXECUTION_ROLE_ARN"],
            "DefinitionS3Location": state_machine_location,
            "DefinitionSubstitutions": None,
            "Tags": [
                {
                    "Key": "bclaw:core-stack-name",
                    "Value": os.environ["CORE_STACK_NAME"],
                },
                {
                    "Key": "bclaw:version",
                    "Value": os.environ["SOURCE_VERSION"],
                },
            ],
        },
    }

    state_machine_logical_name = make_logical_name("main.state.machine")

    yield Resource(state_machine_logical_name, ret)
    return state_machine_logical_name


def state_machine_version_rc(state_machine: Resource) -> Resource:
    ret = {
        "Type": "AWS::StepFunctions::StateMachineVersion",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "Description": "No description",
            "StateMachineArn": {"Ref": state_machine.name},
            "StateMachineRevisionId": {"Fn::GetAtt": [state_machine.name, "StateMachineRevisionId"]},
        },
    }

    return Resource(STATE_MACHINE_VERSION_NAME, ret)


def state_machine_alias_rc(state_machine_version: Resource) -> Resource:
    ret = {
        "Type": "AWS::StepFunctions::StateMachineAlias",
        "Properties": {
            "Name": "current",
            "Description": "Current active version",
            "DeploymentPreference": {
                "StateMachineVersionArn": {"Ref": state_machine_version.name},
                "Type": "ALL_AT_ONCE",
            },
        },
    }

    return Resource(STATE_MACHINE_ALIAS_NAME, ret)


def add_definition_substitutions(sfn_resource: Resource, other_resources: dict) -> None:
    # job definition logical names
    defn_subs = {k: {"Ref": k} for k in other_resources.keys() if k != sfn_resource.name}

    # used in lambda logging block
    defn_subs["WorkflowName"] = {"Ref": "AWS::StackName"}

    # these are used to construct batch job queue arn, subpipe state machine arn
    defn_subs["AWSRegion"] = {"Ref": "AWS::Region"}
    defn_subs["AWSAccountId"] = {"Ref": "AWS::AccountId"}

    sfn_resource.spec["Properties"]["DefinitionSubstitutions"] = defn_subs

from collections import deque
import json
import logging
from typing import Generator, List, Dict, Tuple
from uuid import uuid4

import boto3

from . import batch_resources as b
from . import chooser_resources as c
from . import enhanced_parallel_resources as ep
from . import misc_resources as m
from . import native_step_resources as ns
from . import scatter_gather_resources as sg
from . import subpipe_resources as sp
from .util import CoreStack, Step, Resource, State, make_logical_name, lambda_logging_block, lambda_retry
from .validation import validate_batch_step, validate_native_step, validate_parallel_step, validate_scatter_step, \
    validate_subpipe_step, validate_chooser_step


def make_initializer_step(core_stack: CoreStack, wf_params: dict) -> dict:
    initialize_step_name = "Initialize"

    ret = {
        initialize_step_name: {
            "Type": "Task",
            "Resource": core_stack.output("InitializerLambdaArn"),
            "Parameters": {
                "repo_template": wf_params["repository"],
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


def process_step(core_stack: CoreStack,
                 step: Step,
                 wf_params: dict,
                 depth: int) -> Generator[Resource, None, List[State]]:
    if "scatter" in step.spec:
        normalized_step = validate_scatter_step(step)
        states_to_add = yield from sg.handle_scatter_gather(core_stack,
                                                            normalized_step,
                                                            wf_params,
                                                            depth)

    elif "image" in step.spec:
        normalized_step = validate_batch_step(step)
        scattered = depth > 0
        states_to_add = yield from b.handle_batch(core_stack,
                                                  normalized_step,
                                                  wf_params,
                                                  scattered)

    elif "subpipe" in step.spec:
        normalized_step = validate_subpipe_step(step)
        states_to_add = sp.handle_subpipe(core_stack,
                                          normalized_step)

    elif "Type" in step.spec:
        normalized_step = validate_native_step(step)
        states_to_add = yield from ns.handle_native_step(core_stack,
                                                         normalized_step,
                                                         wf_params,
                                                         depth)

    elif "choices" in step.spec:
        normalized_step = validate_chooser_step(step)
        states_to_add = c.handle_chooser_step(core_stack,
                                              normalized_step)

    elif "branches" in step.spec:
        normalized_step = validate_parallel_step(step)
        states_to_add = yield from ep.handle_parallel_step(core_stack,
                                                           normalized_step,
                                                           wf_params,
                                                           depth)

    else:
        raise RuntimeError(f"step '{step.name}' is not a recognized step type")

    return states_to_add


def make_branch(core_stack: CoreStack,
                raw_steps: list,
                wf_params: dict,
                include_initializer: bool = False,
                depth: int = 0) -> Generator[Resource, None, dict]:
    logger = logging.getLogger(__name__)

    if include_initializer:
        initializer_step = make_initializer_step(core_stack, wf_params)
        raw_steps.insert(0, initializer_step)

    steps = make_step_list(raw_steps)
    states = {}

    for step in steps:
        states_to_add = yield from process_step(core_stack, step, wf_params, depth)
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


def write_state_machine_to_s3(sfn_def: dict, core_stack: CoreStack) -> dict:
    def_json = json.dumps(sfn_def, indent=4)

    bucket = core_stack.output("ResourceBucketName")

    base_filename = uuid4().hex
    key = f"stepfunctions/{base_filename}.json"

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


def handle_state_machine(core_stack: CoreStack,
                         raw_steps: List[Dict],
                         wf_params: dict,
                         dst_fh=None) -> Generator[Resource, None, str]:
    state_machine_def = yield from make_branch(core_stack, raw_steps, wf_params, include_initializer=True)

    if dst_fh is None:
        state_machine_location = write_state_machine_to_s3(state_machine_def, core_stack)
    else:
        state_machine_location = write_state_machine_to_fh(state_machine_def, dst_fh)

    state_machine_name = make_logical_name("main.state.machine")
    ret = {
        "Type": "AWS::StepFunctions::StateMachine",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "StateMachineName": {
                "Fn::Sub": [
                    "${Root}--${Version}",
                    {
                        "Root": {"Ref": "AWS::StackName"},
                        "Version": {
                            "Fn::GetAtt": [m.LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaVersion"]
                        }
                    }
                ]
            },
            "RoleArn": core_stack.output("StatesExecutionRoleArn"),
            "DefinitionS3Location": state_machine_location,
            "DefinitionSubstitutions": None,
            "Tags": [
                {
                    "Key": "bclaw:core-stack-name",
                    "Value": core_stack.name,
                },
            ],
        },
    }

    yield Resource(state_machine_name, ret)
    return state_machine_name


def add_definition_substitutions(sfn_resource: Resource, other_resources: dict) -> None:
    defn_subs = {k: {"Ref": k} for k in other_resources.keys() if k != sfn_resource.name}
    defn_subs["WorkflowName"] = {"Ref": "AWS::StackName"}
    defn_subs["AWSRegion"] = {"Ref": "AWS::Region"}
    defn_subs["AWSAccountId"] = {"Ref": "AWS::AccountId"}

    sfn_resource.spec["Properties"]["DefinitionSubstitutions"] = defn_subs

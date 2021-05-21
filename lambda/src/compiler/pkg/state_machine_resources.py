from collections import deque
import json
import logging
from typing import Generator, Tuple, Iterable, List, Dict
from uuid import uuid4

import boto3
from more_itertools import peekable

from . import batch_resources as b
from . import chooser_resources as c
from . import enhanced_parallel_resources as ep
from . import native_step_resources as ns
from . import scatter_gather_resources as sg
from . import subpipe_resources as sp
from .util import CoreStack, Resource, State, SENTRY, make_logical_name, lambda_logging_block
from .util import Step2 as Step
from .validation import validate_batch_step, validate_native_step, validate_parallel_step, validate_scatter_step, \
    validate_subpipe_step, validate_chooser_step


def make_launcher_step(core_stack: CoreStack, wf_params: dict) -> dict:
    launch_step_name = "Launch"

    ret = {
        launch_step_name: {
            "Type": "Task",
            "Resource": core_stack.output("LauncherLambdaArn"),
            "Parameters": {
                "repo_template": wf_params["repository"],
                "input_obj.$": "$",
                **lambda_logging_block(launch_step_name),
            },
            "ResultPath": "$",
            "OutputPath": "$",
            "_stet": True,
        },
    }

    return ret


# def _stepperator(steps: Iterable) -> Generator[Step, None, None]:
#     for record in steps:
#         name, spec = next(iter(record.items()))
#         step = Step(name, spec)
#         yield step


# def capitalize_key_inplace(d: dict, k: str) -> None:
#     try:
#         d[k.capitalize()] = d.pop(k)
#     except KeyError:
#         pass


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


# def fill_in_nexts_and_ends(steps: list) -> None:
#     next_step = None
#
#     for step in _stepperator(reversed(steps)):
#         capitalize_key_inplace(step.spec, "next")
#         capitalize_key_inplace(step.spec, "end")
#
#         if "Next" not in step.spec and "End" not in step.spec:
#             if next_step is None:
#                 step.spec.update({"End": True})
#             else:
#                 step.spec.update({"Next": next_step.name})
#
#         next_step = step


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
        states_to_add = yield from b.handle_batch(core_stack,
                                                  normalized_step,
                                                  wf_params)

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
                include_launcher: bool = False,
                depth: int = 0) -> Generator[Resource, None, dict]:
    logger = logging.getLogger(__name__)

    if include_launcher:
        launcher_step = make_launcher_step(core_stack, wf_params)
        raw_steps.insert(0, launcher_step)

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


# def make_branch0(core_stack: CoreStack,
#                 steps: list,
#                 wf_params: dict,
#                 include_launcher: bool = False,
#                 depth: int = 0) -> Generator[Resource, None, dict]:
#
#     logger = logging.getLogger(__name__)
#
#     step_iter = peekable(_stepperator(steps))
#
#     first_step_name = step_iter.peek(SENTRY).name
#
#     if include_launcher:
#         first_step_name, states = make_launcher_step(core_stack, wf_params, first_step_name)
#     else:
#         states = {}
#
#     prev_outputs = {}
#
#     for step in step_iter:
#         next_step = step_iter.peek(SENTRY)
#
#         if "scatter" in step.spec:
#             normalized_spec = validate_scatter_step(step)
#             prev_outputs, steps_to_add = yield from sg.handle_scatter_gather(core_stack,
#                                                                              step.name,
#                                                                              normalized_spec,
#                                                                              wf_params,
#                                                                              prev_outputs,
#                                                                              next_step,
#                                                                              depth)
#
#         elif "image" in step.spec:
#             normalized_spec = validate_batch_step(step)
#             prev_outputs, steps_to_add = yield from b.handle_batch(core_stack,
#                                                                    step.name,
#                                                                    normalized_spec,
#                                                                    wf_params,
#                                                                    prev_outputs,
#                                                                    next_step)
#
#         elif "subpipe" in step.spec:
#             normalized_spec = validate_subpipe_step(step)
#             steps_to_add = sp.handle_subpipe(core_stack,
#                                              step.name,
#                                              normalized_spec,
#                                              next_step)
#
#         elif "Type" in step.spec:
#             normalized_spec = validate_native_step(step)
#             steps_to_add = yield from ns.handle_native_step(core_stack,
#                                                             step.name,
#                                                             normalized_spec,
#                                                             wf_params,
#                                                             next_step,
#                                                             depth)
#
#         elif "choices" in step.spec:
#             normalized_spec = validate_chooser_step(step)
#             steps_to_add = c.handle_chooser_step(core_stack,
#                                                  step.name,
#                                                  normalized_spec,
#                                                  next_step)
#
#         elif "branches" in step.spec:
#             normalized_spec = validate_parallel_step(step)
#             steps_to_add = ep.handle_parallel_step(core_stack,
#                                                    step.name,
#                                                    normalized_spec,
#                                                    wf_params,
#                                                    next_step,
#                                                    depth)
#
#         else:
#             raise RuntimeError(f"step '{step.name}' is not a recognized step type")
#
#         states.update(steps_to_add)
#
#     ret = {
#         "StartAt": first_step_name,
#         "States": states,
#     }
#
#     return ret


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
    state_machine_def = yield from make_branch(core_stack, raw_steps, wf_params, include_launcher=True)

    if dst_fh is None:
        state_machine_location = write_state_machine_to_s3(state_machine_def, core_stack)
    else:
        state_machine_location = write_state_machine_to_fh(state_machine_def, dst_fh)

    state_machine_name = make_logical_name("main.state.machine")
    ret = {
        "Type": "AWS::StepFunctions::StateMachine",
        "Properties": {
            "StateMachineName": {
                "Ref": "AWS::StackName",
            },
            "RoleArn": core_stack.output("StatesExecutionRoleArn"),
            "DefinitionS3Location": state_machine_location,
            "DefinitionSubstitutions": None,
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

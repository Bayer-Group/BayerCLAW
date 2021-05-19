import json
import logging
from typing import Generator, List

from . import state_machine_resources as sm
from .util import CoreStack, Step, Resource, State, do_param_substitution, lambda_logging_block


def scatter_step(core_stack: CoreStack, step: Step, map_step_name: str) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("ScatterLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "scatter": json.dumps(step.spec["scatter"]),
            **step.input_field,
            **lambda_logging_block(step.name),
        },
        "ResultPath": "$.items",
        "Next": map_step_name
    }

    return ret


def map_step(sub_branch: dict, gather_step_name: str) -> dict:
    ret = {
        "Type": "Map",
        "ItemsPath": "$.items",
        "Parameters": {
            "id_prefix.$": "$.id_prefix",
            "index.$": "States.Format('{}', $$.Map.Item.Index)",  # stringify the index
            "job_file.$": "$.job_file",
            "prev_outputs": {},
            "repo.$": "$$.Map.Item.Value.repo",
        },
        "Iterator": sub_branch,
        "ResultPath": "$.results",
        "Next": gather_step_name,
    }

    return ret


def gather_step(core_stack: CoreStack, step: Step) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("GatherLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "outputs": json.dumps(step.spec["outputs"]),
            "results.$": "$.results",
            **lambda_logging_block(step.name),
        },
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **step.next_or_end,
    }

    return ret


def handle_scatter_gather(core_stack: CoreStack,
                          step: Step,
                          wf_params: dict,
                          map_depth: int
                          ) -> Generator[Resource, None, List[State]]:
    logger = logging.getLogger(__name__)
    logger.info(f"making scatter gather steps for {step.name}")

    if map_depth > 0:
        raise RuntimeError("Nested Scatter steps are not supported")

    subbed_spec = do_param_substitution(step.spec)
    subbed_step = Step(step.name, subbed_spec)

    sub_branch = yield from sm.make_branch(core_stack, subbed_step.spec["steps"], wf_params, depth=map_depth + 1)

    scatter_step_name = subbed_step.name
    map_step_name = f"{subbed_step.name}.map"
    gather_step_name = f"{subbed_step.name}.gather"

    ret = [
        State(scatter_step_name, scatter_step(core_stack, subbed_step, map_step_name)),
        State(map_step_name, map_step(sub_branch, gather_step_name)),
        State(gather_step_name, gather_step(core_stack, subbed_step))
    ]

    return ret

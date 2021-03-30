import json
import logging
from typing import Generator, List, Tuple

from . import state_machine_resources as sm
from .util import CoreStack, Step, Resource, State, next_or_end, do_param_substitution, lambda_logging_block


def scatter_step(core_stack: CoreStack, step_name: str, spec: dict, next_step_name: str) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("ScatterLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "scatter": json.dumps(spec["scatter"]),
            "inputs": json.dumps(spec["inputs"]),
            **lambda_logging_block(step_name),
        },
        "ResultPath": "$.items",
        "Next": next_step_name
    }

    return ret


def map_step(sub_branch: dict, next_step_name: str) -> dict:
    ret = {
        "Type": "Map",
        "ItemsPath": "$.items",
        "Parameters": {
            "id_prefix.$": "$.id_prefix",
            "index.$": "States.Format('{}', $$.Map.Item.Index)",  # stringify the index
            "job_file.$": "$.job_file",
            "repo.$": "$$.Map.Item.Value.repo",
        },
        "Iterator": sub_branch,
        "ResultPath": "$.results",
        "Next": next_step_name,
    }

    return ret


def gather_step(core_stack: CoreStack, step_name: str, spec: dict, next_step: Step) -> dict:
    ret = {
        "Type": "Task",
        "Resource": core_stack.output("GatherLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "outputs": json.dumps(spec["outputs"]),
            "results.$": "$.results",
            **lambda_logging_block(step_name),
        },
        "ResultPath": "$.manifest",
        **next_or_end(next_step)
    }

    return ret


def handle_scatter_gather(core_stack: CoreStack,
                          step_name: str,
                          spec: dict,
                          wf_params: dict,
                          prev_outputs: dict,
                          next_step: Step,
                          map_depth: int
                          ) -> Generator[Resource, None, Tuple[dict, List[State]]]:
    logger = logging.getLogger(__name__)
    logger.info(f"making scatter gather steps for {step_name}")

    if map_depth > 0:
        raise RuntimeError("Nested Scatter steps are not supported")

    subbed_spec = do_param_substitution(spec)

    if subbed_spec["inputs"] is None:
        subbed_spec["inputs"] = prev_outputs

    sub_branch = yield from sm.make_branch(core_stack, subbed_spec["steps"], wf_params, depth=map_depth + 1)

    scatter_step_name = step_name  # this is what previous step will be expecting
    map_step_name = f"{step_name}.map"
    gather_step_name = f"{step_name}.gather"

    ret = [
        State(scatter_step_name, scatter_step(core_stack, step_name, subbed_spec, map_step_name)),
        State(map_step_name, map_step(sub_branch, gather_step_name)),
        State(gather_step_name, gather_step(core_stack, step_name, subbed_spec, next_step))
    ]

    return subbed_spec["outputs"], ret

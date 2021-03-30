import logging
from typing import Generator, List

from . import state_machine_resources as sm
from .util import CoreStack, Step, Resource, State, next_or_end


def handle_native_step(core_stack: CoreStack,
                       step_name: str,
                       spec: dict,
                       wf_params: dict,
                       next_step: Step,
                       map_depth: int) -> Generator[Resource, None, List[State]]:
    logger = logging.getLogger(__name__)
    logger.info(f"making native step {step_name}")

    ret = spec.copy()

    if spec["Type"] == "Parallel":
        sub_branches = []
        for branch in spec["Branches"]:
            sub_branch = yield from sm.make_branch(core_stack, branch["steps"], wf_params, depth=map_depth)
            sub_branches.append(sub_branch)

        ret.update({"Branches": sub_branches})

    if spec["Type"] not in {"Choice", "Wait", "Succeed", "Fail"}:
        ret.update({"ResultPath": None})

    if spec["Type"] != "Fail":
        ret.update({"OutputPath": "$"})

    ret.pop("End", None)

    if spec["Type"] not in {"Succeed", "Fail"}:
        ret.update(next_or_end(next_step))

    return [State(step_name, ret)]

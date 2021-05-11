import logging
from typing import Generator, List

from . import state_machine_resources as sm
from .util import CoreStack, Step, Resource, State, next_or_end, lambda_logging_block


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
        inputs = ret.pop("inputs")

        for idx, branch in enumerate(spec["Branches"], start=1):
            steps = branch["steps"]
            condition = branch.get("if", None)

            if condition is not None:
                next_step_name = next(iter(steps[0]))
                skip_step_name = f"{step_name}: skip_{idx}"

                # note: this creates two native-type steps in the BayerCLAW spec language.
                # They will be processed into Amazon States Language in the sm.make_branch()
                # call below.
                preamble = [
                    {
                        f"{step_name}: {condition}?": {
                            "Type": "Task",
                            "Resource": core_stack.output("ChooserLambdaArn"),
                            "Parameters": {
                                "repo.$": "$.repo",
                                "inputs": inputs,
                                "expression": condition,
                                **lambda_logging_block(step_name)
                            },
                            "Catch": [
                                {
                                    "ErrorEquals": ["ConditionFailed"],
                                    "Next": skip_step_name,
                                },
                            ],
                            # don't have to do the next_or_end thing, per validation there
                            # has to be a next step
                            "Next": next_step_name,
                        },
                    },
                    {
                        skip_step_name: {
                            "Type": "Succeed",
                        },
                    },
                ]

                steps = preamble + steps

            sub_branch = yield from sm.make_branch(core_stack, steps, wf_params, depth=map_depth)
            sub_branches.append(sub_branch)

        ret.update({"Branches": sub_branches})

    if spec["Type"] not in {"Choice", "Wait", "Succeed", "Fail"}:
        ret.update({"ResultPath": None})

    if spec["Type"] != "Fail":
        ret.update({"OutputPath": "$"})

    ret.pop("End", None)

    # todo: need to work with "next" or "Next" (handle in next_or_end?)
    if spec["Type"] not in {"Succeed", "Fail"} and "Next" not in spec:  # ???
        ret.update(next_or_end(next_step))

    return [State(step_name, ret)]

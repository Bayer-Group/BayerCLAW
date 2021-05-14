from typing import Generator, List

from . import state_machine_resources as sm
from .util import CoreStack, Step, Resource, State, next_or_end, lambda_logging_block


def handle_parallel_step(core_stack: CoreStack,
                         step_name: str,
                         spec: dict,
                         wf_params: dict,
                         next_step: Step,
                         map_depth: int) -> Generator[Resource, None, List[State]]:
    inputs = spec["inputs"]
    sfn_branches = []

    for idx, branch in enumerate(spec["branches"], start=1):
        steps = branch["steps"]
        try:
            expression = branch["if"]
            next_step_name = next(iter(steps[0]))
            skip_step_name = f"{step_name}: skip_{idx}"

            # note: this creates two native-type steps in the BayerCLAW spec language.
            # They will be processed into Amazon States Language in the sm.make_branch()
            # call below.
            preamble = [
                {
                    f"{step_name}: {expression}?": {
                        "Type": "Task",
                        "Resource": core_stack.output("ChooserLambdaArn"),
                        "Parameters": {
                            "repo.$": "$.repo",
                            "inputs": inputs,
                            "expression": expression,
                            **lambda_logging_block(step_name)
                        },
                        "Catch": [
                            {
                                "ErrorEquals": ["ConditionFailed"],
                                "Next": skip_step_name
                            },
                        ],
                        "ResultPath": None,
                        "OutputPath": "$",
                        "_stet": True,

                        # don't have to do the next_or_end thing, per validation there
                        # has to be a next step
                        "Next": next_step_name,
                    },
                },
                {
                    skip_step_name: {
                        "Type": "Succeed",
                        "_stet": True,
                    },
                },
            ]

            steps = preamble + steps

        except KeyError:
            pass

        sfn_branch = yield from sm.make_branch(core_stack, steps, wf_params, depth=map_depth)
        sfn_branches.append(sfn_branch)

    ret = {
        "Type": "Parallel",
        "Branches": sfn_branches,
        "ResultPath": None,
        "OutputPath": "$",
        **next_or_end(next_step)
    }

    return [State(step_name, ret)]
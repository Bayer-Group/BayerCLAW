import jmespath
from typing import List

from .util import CoreStack, Step, State, lambda_logging_block, SENTRY


def choice_spec(expr_str: str, next_step: str) -> dict:
    ret = {
        "Variable": "$.choice",
        "StringEquals": expr_str,
        "Next": next_step
    }
    return ret


# todo: need logging
def handle_chooser_step(core_stack: CoreStack, step_name: str, spec: dict, next_step: Step) -> List[State]:
    choice_step_name = f"{step_name}.choose"

    exprs = jmespath.search("choices[].if", spec)
    nexts = jmespath.search("choices[].next", spec)

    choices = [choice_spec(e, n) for e, n in zip(exprs, nexts)]

    task_step = {
        "Type": "Task",
        "Resource": core_stack.output("ChooserLambdaArn"),
        "Parameters": {
            "repo.$": "$.repo",
            "inputs": spec["inputs"],
            "expressions": exprs,
            **lambda_logging_block(step_name),
        },
        "ResultPath": "$.choice",
        "OutputPath": "$",
        "Next": choice_step_name,
    }

    choice_step = {
        "Type": "Choice",
        "Choices": choices,
    }

    if next_step is not SENTRY:
        choice_step.update({"Default": next_step.name})

    ret = [
        State(step_name, task_step),
        State(choice_step_name, choice_step),
    ]

    return ret

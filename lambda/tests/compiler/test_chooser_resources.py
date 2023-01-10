import json
import logging
import pytest

from voluptuous.error import Invalid

logging.basicConfig(level=logging.INFO)

from ...src.compiler.pkg.chooser_resources import choice_spec, handle_chooser_step
from ...src.compiler.pkg.util import Step, State, lambda_logging_block, lambda_retry


def test_choice_spec():
    condition = "x == 1"
    next_step = "step99"

    result = choice_spec(condition, next_step)
    expect = {
        "Variable": "$.choice",
        "StringEquals": condition,
        "Next": next_step,
    }
    assert result == expect


def test_make_chooser_steps(mock_core_stack, compiler_env):
    spec = {
        "inputs": {
            "infile1": "file1.json",
            "infile2": "file2.json",
        },
        "choices": [
            {
                "if": "infile1.var1 == 1",
                "next": "step99",
            },
            {
                "if": "infile2.var2 == 2",
                "next": "step98",
            },
            {
                "if": "job.var3 == 3",
                "next": "step97",
            },
        ]
    }

    test_step = Step("step_name", spec, "next_step")

    expected_task_spec = {
        "Type": "Task",
        "Resource": "chooser_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo",
            "inputs": json.dumps(spec["inputs"]),
            "expressions": [
                "infile1.var1 == 1",
                "infile2.var2 == 2",
                "job.var3 == 3",
            ],
            **lambda_logging_block("step_name")
        },
        **lambda_retry(),
        "ResultPath": "$.choice",
        "OutputPath": "$",
        "Next": "step_name.choose",
    }

    expected_choice_spec = {
        "Type": "Choice",
        "Choices": [
            {
                "Variable": "$.choice",
                "StringEquals": "infile1.var1 == 1",
                "Next": "step99",
            },
            {
                "Variable": "$.choice",
                "StringEquals": "infile2.var2 == 2",
                "Next": "step98",
            },
            {
                "Variable": "$.choice",
                "StringEquals": "job.var3 == 3",
                "Next": "step97",
            },
        ],
        "Default": "next_step"
    }

    result = handle_chooser_step(test_step)
    assert len(result) == 2
    assert all(isinstance(s, State) for s in result)

    task_state = result[0]
    assert task_state.name == "step_name"
    assert task_state.spec == expected_task_spec

    choice_state = result[1]
    assert choice_state.name == "step_name.choose"
    assert choice_state.spec == expected_choice_spec


def test_make_chooser_steps_terminal_state_fail():
    test_step = Step("step_name", {"not": "used"}, "")
    with pytest.raises(Invalid, match="chooser steps cannot be terminal"):
        _ = handle_chooser_step(test_step)

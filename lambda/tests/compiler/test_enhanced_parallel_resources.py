import json
import textwrap

import pytest
import yaml

from ...src.compiler.pkg.enhanced_parallel_resources import handle_parallel_step
from ...src.compiler.pkg.util import Step, State, lambda_logging_block, lambda_retry


@pytest.mark.parametrize("next_step_name, next_or_end", [
    ("next_step", {"Next": "next_step"}),
    ("", {"End": True}),
])
def test_handle_parallel_step_enhanced(next_step_name, next_or_end, compiler_env):
    spec_yaml = textwrap.dedent("""\
      inputs:
        input1: file1.json
        input2: file2.json
      branches:
        -
          if: input1.qc == 1
          steps:
            -
              do_this:
                Type: Pass
            -
              do_that:
                Type: Pass
        -
          if: input2.qc == 2
          steps:
            -
              do_the_other:
                Type: Pass
        -
          steps:
            -
              always_do_this:
                Type: Pass
            -
              this_too:
                Type: Pass
    """)

    spec = yaml.safe_load(spec_yaml)
    options = {"wf": "params"}

    def helper():
        step = Step("step_name", spec, next_step_name)

        result, *more = yield from handle_parallel_step(step, options, 0)
        assert len(more) == 0
        assert isinstance(result, State)
        assert result.spec["Type"] == "Parallel"
        assert result.spec["ResultPath"] is None
        assert result.spec["OutputPath"] == "$"
        assert next_or_end.items() <= result.spec.items()
        assert len(result.spec["Branches"]) == 3

        # branch "1"
        branch_1 = result.spec["Branches"][0]
        condition_1 = step.spec["branches"][0]["if"]
        check_step_name_1 = f"step_name: {condition_1}?"
        skip_step_name_1 = "step_name: skip_1"
        assert branch_1["StartAt"] == check_step_name_1
        assert set(branch_1["States"].keys()) == {check_step_name_1, skip_step_name_1, "do_this", "do_that"}

        expected_inputs = json.dumps(step.spec["inputs"])

        # -- check step 1
        check_1 = branch_1["States"][check_step_name_1]
        expect_1 = {
            "Type": "Task",
            "Resource": "chooser_lambda_arn",
            "Parameters": {
                "repo.$": "$.repo",
                "inputs": expected_inputs,
                "expression": condition_1,
                **lambda_logging_block("step_name")
            },
            **lambda_retry(),
            "Catch": [
                {
                    "ErrorEquals": ["ConditionFailed"],
                    "Next": skip_step_name_1,
                },
            ],
            "ResultPath": None,
            "OutputPath": "$",
            "Next": "do_this",
        }
        assert check_1 == expect_1

        # -- skip step 1
        skip_branch_1 = branch_1["States"][skip_step_name_1]
        assert skip_branch_1["Type"] == "Succeed"

        # branch "2"
        branch_2 = result.spec["Branches"][1]
        condition_2 = step.spec["branches"][1]["if"]
        check_step_name_2 = f"step_name: {condition_2}?"
        skip_step_name_2 = "step_name: skip_2"
        assert branch_2["StartAt"] == check_step_name_2
        assert set(branch_2["States"].keys()) == {check_step_name_2, skip_step_name_2, "do_the_other"}

        # -- step check_2
        check_2 = branch_2["States"][check_step_name_2]
        expect_2 = {
            "Type": "Task",
            "Resource": "chooser_lambda_arn",
            "Parameters": {
                "repo.$": "$.repo",
                "inputs": expected_inputs,
                "expression": condition_2,
                **lambda_logging_block("step_name")
            },
            **lambda_retry(),
            "Catch": [
                {
                    "ErrorEquals": ["ConditionFailed"],
                    "Next": skip_step_name_2,
                },
            ],
            "ResultPath": None,
            "OutputPath": "$",
            "Next": "do_the_other",
        }
        assert check_2 == expect_2

        # -- step skip_branch_2
        skip_branch_2 = branch_2["States"][skip_step_name_2]
        assert skip_branch_2["Type"] == "Succeed"

        # branch "3"
        branch_3 = result.spec["Branches"][2]
        assert branch_3["StartAt"] == "always_do_this"
        assert set(branch_3["States"].keys()) == {"always_do_this", "this_too"}

    _ = list(helper())

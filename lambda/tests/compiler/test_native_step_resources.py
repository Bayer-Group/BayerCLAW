import textwrap

import pytest
import yaml

from ...src.compiler.pkg.native_step_resources import handle_native_step
from ...src.compiler.pkg.util import Step, State


pass_test = {
    "Type": "Pass",
    "Comment": "test case for Pass state",
    "InputPath": "$.input_path",
    "Parameters": {"x": 1},
    "Result": {"y": 1},
    "ResultPath": "override_this",
    "OutputPath": "override_this",
    "Next": "override_this",
    "End": False,
}

task_test = {
    "Type": "Task",
    "Comment": "test case for Task state",
    "Resource": "arn:etc:etc",
    "InputPath": "$.input_path",
    "Parameters": {"x": 1},
    "ResultPath": "override_this",
    "OutputPath": "override_this",
    "Next": "override_this",
    "End": False,
}

wait_test = {
    "Type": "Wait",
    "Comment": "test case for Wait state",
    "Seconds": "99",
    "SecondsPath": "$.seconds",
    "Timestamp": "2020-04-23T12:41:00Z",
    "TimestampPath": "$.timestamp",
    "InputPath": "$.input_path",
    "OutputPath": "override_this",
    "Next": "override_this",
    "End": False,
}

succeed_test = {
    "Type": "Succeed",
    "Comment": "test case for Succeed state",
    "InputPath": "$.input_path",
    "OutputPath": "override_this",
}

fail_test = {
    "Type": "Fail",
    "Comment": "test case for Fail state",
    "Error": "failure error",
    "Cause": "it failed",
}


@pytest.mark.parametrize("test_input, next_or_end", [
    (pass_test, "next_step"),
    (pass_test, ""),
    (task_test, "next_step"),
    (task_test, ""),
    (wait_test, "next_step"),
    (wait_test, ""),
    (succeed_test, "unused"),
    (fail_test, "unused"),
])
def test_handle_native_step(test_input, next_or_end):
    options = {"wf": "options"}

    def helper():
        test_step = Step("step_name", test_input, next_or_end)
        result, *more = yield from handle_native_step(test_step, options, 0)
        assert len(more) == 0
        assert isinstance(result, State)
        assert result.name == "step_name"

        try:
            result_path = result.spec.pop("ResultPath")
            assert result_path is None
        except KeyError:
            pass

        try:
            output_path = result.spec.pop("OutputPath")
            assert output_path == "$"
        except KeyError:
            pass

        if result.spec["Type"] in {"Succeed", "Fail"}:
            assert "Next" not in result
            assert "End" not in result
        else:
            if next_or_end == "":
                end_value = result.spec.pop("End")
                assert end_value is True
                assert "Next" not in result
            else:
                next_value = result.spec.pop("Next")
                assert next_value == next_or_end
                assert "End" not in result

        # make sure all other fields are unchanged
        assert result.spec.items() <= test_input.items()

    resources = list(helper())
    assert len(resources) == 0


def test_handle_native_step_stet():
    test_input = {
        "Type": "AnyType",
        "ResultPath": "keep_this_result_path",
        "OutputPath": "keep_this_output_path",
        "_stet": True,
        "Other": "stuff",
    }
    options = {"wf": "options"}

    def helper():
        test_step = Step("step_name", test_input, "next_step")
        result, *more = yield from handle_native_step(test_step, options, 0)
        expect = {
            "Type": "AnyType",
            "ResultPath": "keep_this_result_path",
            "OutputPath": "keep_this_output_path",
            "Other": "stuff",
            "Next": "next_step",
        }

        assert len(more) == 0
        assert isinstance(result, State)
        assert result.name == "step_name"
        assert result.spec == expect

    resources = list(helper())
    assert len(resources) == 0


def test_handle_parallel_native_step(compiler_env):
    step_yaml = textwrap.dedent("""
      Type: Parallel
      inputs: {}
      Branches:
        -
          steps:
            -
              do_this:
                image:
                    name: bclaw-blank
                    auth: ""
                references:
                  m: s3://n
                inputs:
                  x: y
                commands:
                  - etc
                outputs:
                  a: b
                s3_tags: {}
                job_tags: {}
                compute:
                  cpus: 1
                  memory: 4
                  spot: true
                skip_if_output_exists: true
            -
              do_that:
                image:
                    name: bclaw-wut
                    auth: ""
                references:
                  m: s3://n
                inputs:
                  p: q
                commands:
                  - stuff
                outputs:
                  z: t
                s3_tags: {}
                job_tags: {}
                compute:
                  cpus: 1
                  memory: 4
                  spot: true
                skip_if_output_exists: true
        -
          steps:
            -
              do_the_other:
                image:
                    name: who-dat
                    auth: ""
                references:
                  m: s3://n
                inputs:
                  u: v
                commands:
                  - woohoo
                outputs:
                  k: l
                s3_tags: {}
                job_tags: {}
                compute:
                  cpus: 1
                  memory: 4
                  spot: true
                skip_if_output_exists: true
      Next: override_this
    """)
    spec = yaml.safe_load(step_yaml)
    options = {"wf": "options",
               "s3_tags": {},
               "job_tags": {}}

    def helper():
        test_step = Step("step_name", spec, "next_step")
        result, *more = yield from handle_native_step(test_step, options, 0)
        # print(str(result))
        assert len(more) == 0
        assert isinstance(result, State)
        assert result.name == "step_name"
        assert result.spec["Type"] == "Parallel"
        assert result.spec["ResultPath"] is None
        assert result.spec["OutputPath"] == "$"
        assert result.spec["Next"] == "next_step"
        assert len(result.spec["Branches"]) == 2
        assert set(result.spec["Branches"][0]["States"].keys()) == {"do_this", "do_that"}
        assert set(result.spec["Branches"][1]["States"].keys()) == {"do_the_other"}

    resources = list(helper())
    for resource in resources:
        assert resource.name in {"DoThisJobDefx", "DoThatJobDefx", "DoTheOtherJobDefx"}
        assert resource.spec["Type"] == "Custom::BatchJobDefinition"

import textwrap

import pytest
import yaml

from ...src.compiler.pkg.native_step_resources import handle_native_step
from ...src.compiler.pkg.util import CoreStack, Step, State


pass_test = {
    "Type": "Pass",
    "Comment": "test case for Pass state",
    "InputPath": "$.input_path",
    "Parameters": {"x": 1},
    "Result": {"y": 1},
    "ResultPath": "$.result_path",
    "OutputPath": "$.output_path",
    "Next": "next_step",
    "End": True,
}

task_test = {
    "Type": "Task",
    "Comment": "test case for Task state",
    "Resource": "arn:etc:etc",
    "InputPath": "$.input_path",
    "Parameters": {"x": 1},
    "ResultPath": "$.result_path",
    "OutputPath": "$.output_path",
    "Next": "next_step",
    "End": True,
}

wait_test = {
    "Type": "Wait",
    "Comment": "test case for Wait state",
    "Seconds": "99",
    "SecondsPath": "$.seconds",
    "Timestamp": "2020-04-23T12:41:00Z",
    "TimestampPath": "$.timestamp",
    "InputPath": "$.input_path",
    "OutputPath": "$.output_path",
    "Next": "next_step",
    "End": True,
}

succeed_test = {
    "Type": "Succeed",
    "Comment": "test case for Succeed state",
    "InputPath": "$.input_path",
    "OutputPath": "$.output_path",
}

fail_test = {
    "Type": "Fail",
    "Comment": "test case for Fail state",
    "Error": "failure error",
    "Cause": "it failed",
}


@pytest.mark.parametrize("test_input", [pass_test, task_test, wait_test, succeed_test, fail_test])
def test_handle_native_step(test_input):
    wf_params = {"wf": "params"}

    def helper():
        result, *more = yield from handle_native_step("core_stack_placeholder", "step_name", test_input, wf_params, Step("next_step", {}), 0)
        assert len(more) == 0
        assert isinstance(result, State)
        assert result.name == "step_name"

        result_path = result.spec.pop("ResultPath", "")
        if result_path != "":
            assert result_path is None

        output_path = result.spec.pop("OutputPath", None)
        if output_path is not None:
            assert output_path == "$"

        if test_input["Type"] in {"Succeed", "Fail"}:
            assert "Next" not in result
        else:
            next_ = result.spec.pop("Next")
            assert next_ == "next_step"

        assert "End" not in result

        assert result.spec.items() <= test_input.items()

    resources = list(helper())
    assert len(resources) == 0


def test_handle_parallel_native_step(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    step_yaml = textwrap.dedent("""
      Type: Parallel
      Branches:
        -
          steps:
            -
              do_this:
                image: bclaw-blank
                params:
                  i: 1
                references:
                  m: s3://n
                inputs:
                  x: y
                commands:
                  - etc
                outputs:
                  a: b
                compute:
                  cpus: 1
                  memory: 4
                  spot: true
                skip_if_output_exists: true
            -
              do_that:
                image: bclaw-wut
                params:
                  i: 1
                references:
                  m: s3://n
                inputs:
                  p: q
                commands:
                  - stuff
                outputs:
                  z: t
                compute:
                  cpus: 1
                  memory: 4
                  spot: true
                skip_if_output_exists: true
        -
          steps:
            -
              do_the_other:
                image: who-dat
                params:
                  i: 1
                references:
                  m: s3://n
                inputs:
                  u: v
                commands:
                  - woohoo
                outputs:
                  k: l   
                compute:
                  cpus: 1
                  memory: 4
                  spot: true
                skip_if_output_exists: true
    """)
    step = yaml.safe_load(step_yaml)
    wf_params = {"wf": "params"}

    def helper():
        result, *more = yield from handle_native_step(core_stack, "step_name", step, wf_params, Step("next_step", {}), 0)
        print(str(result))
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
        assert resource.name in {"DoThisJobDef", "DoThatJobDef", "DoTheOtherJobDef"}
        assert resource.spec["Type"] == "AWS::Batch::JobDefinition"

import json
import textwrap

import pytest
import yaml

from ...src.compiler.pkg.scatter_gather_resources import scatter_step, map_step, gather_step, handle_scatter_gather
from ...src.compiler.pkg.util import CoreStack
from ...src.compiler.pkg.util import Step2 as Step


def test_scatter_step(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    spec = {
        "scatter": {
            "stuff": "test*.txt",
        },
        "params": {
            "a": 1,
            "b": 2,
        },
        "inputs": {
            "input1": "infile1.txt",
            "input2": "infile2.txt",
        },
    }

    test_step = Step("test_step", spec, "unused")
    result = scatter_step(core_stack, test_step, "map_step_name")
    expect = {
        "Type": "Task",
        "Resource": "scatter_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo",
            "scatter": json.dumps(spec["scatter"]),
            "inputs": json.dumps(spec["inputs"]),
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "job_file_s3_request_id.$": "$.job_file.s3_request_id",
                "sfn_execution_id.$": "$$.Execution.Name",
                "step_name": "test_step",
                "workflow_name": "${WorkflowName}",
            },
        },
        "ResultPath": "$.items",
        "Next": "map_step_name",
    }
    assert result == expect


def test_map_step():
    sub_branch = {"fake": "branch"}
    result = map_step(sub_branch, "gather_step_name")
    expect = {
        "Type": "Map",
        "ItemsPath": "$.items",
        "Parameters": {
            "id_prefix.$": "$.id_prefix",
            "index.$": "States.Format('{}', $$.Map.Item.Index)",
            "job_file.$": "$.job_file",
            "prev_outputs": {},
            "repo.$": "$$.Map.Item.Value.repo",
        },
        "Iterator": sub_branch,
        "ResultPath": "$.results",
        "Next": "gather_step_name",
    }
    assert result == expect


@pytest.mark.parametrize("next_step_name, next_or_end", [
    ("next_step", {"Next": "next_step"}),
    ("", {"End": True}),
])
def test_gather_step(next_step_name, next_or_end, monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    spec = {
        "params": {
            "a": 1,
            "b": 2,
        },
        "outputs": {
            "output1": "outfile1.txt",
            "output2": "outfile2.txt",
        },
    }
    test_step = Step("test_step", spec, next_step_name)

    expect = {
        "Type": "Task",
        "Resource": "gather_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo",
            "outputs": json.dumps(spec["outputs"]),
            "results.$": "$.results",
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "job_file_s3_request_id.$": "$.job_file.s3_request_id",
                "sfn_execution_id.$": "$$.Execution.Name",
                "step_name": "test_step",
                "workflow_name": "${WorkflowName}",
            },
       },
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **next_or_end,
    }
    result = gather_step(core_stack, test_step)
    assert result == expect


@pytest.fixture(scope="module")
def sample_scatter_step():
    ret = yaml.safe_load(textwrap.dedent("""
      scatter:
        stuff: test*.txt
      params:
        a: 1
        b: 2
      inputs:
        input1: infile${a}.txt
        input2: infile${b}.txt
      steps:
        -
          Step1:
            image: test_image
            params:
                c: 3
            references:
                ref1: "s3://ref-bucket/path/to/reference.file"
            inputs:
                input3: ${parent.a}_infile${c}.txt
            commands:
              - ls -l ${scatter.stuff} > ${output3}
            outputs:
                output3: ${parent.b}_outfile${c}.txt
            compute:
                cpus: 1
                memory: 4
                spot: true
            skip_if_output_exists: true
      outputs:
        output1: outfile${a}.txt
        output2: outfile${b}.txt
    """))
    yield ret


def test_handle_scatter_gather(monkeypatch, mock_core_stack, sample_scatter_step):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    wf_params = {"wf": "params"}

    def helper():
        step = Step("step_name", sample_scatter_step, "next_step_name")
        states = yield from handle_scatter_gather(core_stack, step, wf_params, 0)

        assert len(states) == 3

        assert states[0].name == "step_name"
        assert states[1].name == "step_name.map"
        assert states[2].name == "step_name.gather"

        assert "parameters" not in states[0].spec["Parameters"]
        inputs0 = json.loads(states[0].spec["Parameters"]["inputs"])
        assert inputs0["input1"] == "infile1.txt"
        assert inputs0["input2"] == "infile2.txt"

        inputs1 = json.loads(states[1].spec["Iterator"]["States"]["Step1"]["Parameters"]["Parameters"]["inputs"])
        assert inputs1["input3"] == "1_infile3.txt"
        refs1 = json.loads(states[1].spec["Iterator"]["States"]["Step1"]["Parameters"]["Parameters"]["references"])
        assert refs1["ref1"] == "s3://ref-bucket/path/to/reference.file"
        outputs1 = json.loads(states[1].spec["Iterator"]["States"]["Step1"]["Parameters"]["Parameters"]["outputs"])
        assert outputs1["output3"] == "2_outfile3.txt"

        assert "parameters" not in states[2].spec["Parameters"]
        outputs2 = json.loads(states[2].spec["Parameters"]["outputs"])
        assert outputs2["output1"] == "outfile1.txt"
        assert outputs2["output2"] == "outfile2.txt"

    resource_gen = helper()
    resources = list(resource_gen)
    assert len(resources) == 1
    assert resources[0].name == "Step1JobDef"
    assert resources[0].spec["Type"] == "AWS::Batch::JobDefinition"

    commands = json.loads(resources[0].spec["Properties"]["Parameters"]["command"])
    assert commands[0] == "ls -l ${scatter.stuff} > ${output3}"


def test_handle_scatter_gather_too_deep():
    def helper():
        fake_step = Step("fake", {"fake": ""}, "fake_next_step")
        with pytest.raises(RuntimeError, match=r"Nested Scatter steps are not supported"):
            _ = yield from handle_scatter_gather("fake_core_stack", fake_step, {"wf": "params"}, 1)

    _ = list(helper())


def test_handle_scatter_gather_auto_inputs(monkeypatch, mock_core_stack, sample_scatter_step):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    sample_scatter_step["inputs"] = None

    def helper():
        test_step = Step("step_name", sample_scatter_step, "next_step_name")
        states = yield from handle_scatter_gather(core_stack, test_step, {"wf": "params"}, 0)
        assert states[0].spec["Parameters"]["inputs.$"] == "States.JsonToString($.prev_outputs)"

    _ = dict(helper())

import json
import textwrap

import pytest
import yaml

from ...src.compiler.pkg.scatter_gather_resources import (scatter_step, error_tolerance, map_step, scatter_init_step,
                                                          gather_step, handle_scatter_gather)
from ...src.compiler.pkg.util import Step, lambda_retry


def test_scatter_step(compiler_env):
    spec = {
        "scatter": {
            "stuff": "test*.txt",
        },
        "inputs": {
            "input1": "infile1.txt",
            "input2": "infile2.txt",
        },
        "outputs": {
            "output1": "outfile1.txt",
            "output2": "outfile2.txt",
        }
    }

    test_step = Step("test_step", spec, "unused")
    result = scatter_step(test_step, "map_step_name")
    expect = {
        "Type": "Task",
        "Resource": "scatter_lambda_arn",
        "Parameters": {
            "repo.$": "$.repo",
            "scatter": json.dumps(spec["scatter"]),
            "inputs": json.dumps(spec["inputs"], separators=(",", ":")),
            "outputs": json.dumps(spec["outputs"]),
            "step_name": "test_step",
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "sfn_execution_id.$": "$$.Execution.Name",
                "step_name": "test_step",
                "workflow_name": "${WorkflowName}",
            },
        },
        **lambda_retry(),
        "ResultPath": "$.scatter",
        "Next": "map_step_name",
    }
    assert result == expect

@pytest.mark.parametrize("spec, expect", [
    ("99%", {"ToleratedFailurePercentage": 99}),
    (88, {"ToleratedFailureCount": 88}),
])
def test_error_tolerance(spec, expect):
    result = error_tolerance(spec)
    assert result == expect


@pytest.mark.parametrize("err_tol, expect_err_tol", [
    (88, {"ToleratedFailureCount": 88}),
    ("77%", {"ToleratedFailurePercentage": 77}),
    ("0000066%", {"ToleratedFailurePercentage": 66}),  # ridiculous leading zeroes
])
def test_map_step(err_tol, expect_err_tol):
    spec = {
        "max_concurrency": 99,
        "error_tolerance": err_tol,
    }
    step = Step("test-step", spec, "unused")
    sub_branch = {
        "StartAt": "fake",
        "States": [
            {"fake": "branch"},
        ],
    }

    result = map_step(step, sub_branch, "gather_step_name")
    expect = {
        "Type": "Map",
        "MaxConcurrency": 99,
        **expect_err_tol,
        "Label": "teststep",
        "ItemReader": {
            "Resource": "arn:aws:states:::s3:getObject",
            "ReaderConfig": {
                "InputType": "CSV",
                "CSVHeaderLocation": "FIRST_ROW",
            },
            "Parameters": {
                "Bucket.$": "$.scatter.items.bucket",
                "Key.$": "$.scatter.items.key",
            },
        },
        "ItemSelector": {
            "index.$": "States.Format('{}', $$.Map.Item.Index)",
            "job_file.$": "$.job_file",
            "prev_outputs": {},
            "scatter.$": "$$.Map.Item.Value",
            "repo.$": "$.scatter.repo",
            "share_id.$": "$.share_id",
        },
        "ItemProcessor": {
            "ProcessorConfig": {
                "Mode": "DISTRIBUTED",
                "ExecutionType": "STANDARD",
            },
            "StartAt": "fake",
            "States": [
                {"fake": "branch"},
            ],
        },
        "ResultWriter": {
            "Resource": "arn:aws:states:::s3:putObject",
            "Parameters": {
                "Bucket.$": "$.scatter.repo.bucket",
                "Prefix.$": "$.scatter.repo.prefix",
            },
        },
        "ResultPath": None,
        "Next": "gather_step_name",
    }
    assert result == expect


def test_scatter_init_step(compiler_env):
    result = scatter_init_step("parent_step_name")
    expect = {
        "parent_step_name.initialize": {
            "Type": "Task",
            "Resource": "scatter_init_lambda_arn",
            "Parameters": {
                "index.$": "$.index",
                "repo.$": "$.repo",
                "scatter.$": "$.scatter",
                "logging": {
                    "branch.$": "$.index",
                    "job_file_bucket.$": "$.job_file.bucket",
                    "job_file_key.$": "$.job_file.key",
                    "job_file_version.$": "$.job_file.version",
                    "sfn_execution_id.$": "$$.Execution.Name",
                    "step_name": "parent_step_name.initialize",
                    "workflow_name": "${WorkflowName}",
                },
            },
            **lambda_retry(max_attempts=10),
            "ResultPath": "$.repo",
            "_stet": True
        }
    }
    assert result == expect


@pytest.mark.parametrize("next_step_name, next_or_end", [
    ("next_step", {"Next": "next_step"}),
    ("", {"End": True}),
])
def test_gather_step(next_step_name, next_or_end, compiler_env):
    spec = {
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
            "repo.$": "$.repo.uri",
            "outputs": json.dumps(spec["outputs"]),
            "step_name": "test_step",
            "logging": {
                "branch.$": "$.index",
                "job_file_bucket.$": "$.job_file.bucket",
                "job_file_key.$": "$.job_file.key",
                "job_file_version.$": "$.job_file.version",
                "sfn_execution_id.$": "$$.Execution.Name",
                "step_name": "test_step",
                "workflow_name": "${WorkflowName}",
            },
        },
        **lambda_retry(),
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **next_or_end,
    }
    result = gather_step(test_step)
    assert result == expect


@pytest.fixture(scope="module")
def sample_scatter_step():
    ret = yaml.safe_load(textwrap.dedent("""
      scatter:
        stuff: test*.txt
      inputs:
        input1: infile1.txt
        input2: infile2.txt
      steps:
        -
          Step1:
            image: test_image
            references:
                ref1: "s3://ref-bucket/path/to/reference.file"
            inputs:
                input3: 1_infile3.txt
            commands:
              - ls -l ${scatter.stuff} > ${output3}
            outputs:
                output3:
                    name: 2_outfile3.txt
                    s3_tags:
                        s3_tag3: file_s3_value3
                        s3_tag4: file_s3_value4
            s3_tags:
              s3_tag2: step_s3_value2
              s3_tag3: step_s3_value3
            compute:
                cpus: 1
                memory: 4
                spot: true
            skip_if_output_exists: true
      outputs:
        output1: outfile1.txt
        output2: outfile2.txt
      max_concurrency: 99
      error_tolerance: 88
    """))
    yield ret


def test_handle_scatter_gather(sample_scatter_step, compiler_env):
    options = {
        "wf": "options",
        "s3_tags": {
            "s3_tag1": "global_s3_value1",
            "s3_tag2": "global_s3_value2",
        },
        "job_tags": {
            "job_tag1": "global_job_value1",
            "job_tag2": "global_job_value2",
        }
    }

    def helper():
        step = Step("step_name", sample_scatter_step, "next_step_name")
        states = yield from handle_scatter_gather(step, options, 0)

        assert len(states) == 3

        assert states[0].name == "step_name"
        assert states[1].name == "step_name.map"
        assert states[2].name == "step_name.gather"

        inputs0 = json.loads(states[0].spec["Parameters"]["inputs"])
        assert inputs0["input1"] == "infile1.txt"
        assert inputs0["input2"] == "infile2.txt"

        inputs1 = json.loads(states[1].spec["ItemProcessor"]["States"]["Step1"]["Parameters"]["Parameters"]["inputs"])
        assert inputs1["input3"] == "1_infile3.txt"
        refs1 = json.loads(states[1].spec["ItemProcessor"]["States"]["Step1"]["Parameters"]["Parameters"]["references"])
        assert refs1["ref1"] == "s3://ref-bucket/path/to/reference.file"
        outputs1 = json.loads(states[1].spec["ItemProcessor"]["States"]["Step1"]["Parameters"]["Parameters"]["outputs"])
        assert outputs1["output3"] == {"name": "2_outfile3.txt", "s3_tags": {"s3_tag3": "file_s3_value3", "s3_tag4": "file_s3_value4"}}

        assert len(states[1].spec["ItemProcessor"]["States"]) == 2
        assert set(states[1].spec["ItemProcessor"]["States"]) == {"step_name.initialize", "Step1"}
        assert states[1].spec["ItemProcessor"]["StartAt"] == "step_name.initialize"

        substep1 = states[1].spec["ItemProcessor"]["States"]["step_name.initialize"]
        assert substep1["Type"] == "Task"
        assert substep1["Resource"] == "scatter_init_lambda_arn"
        assert substep1["Next"] == "Step1"

        substep2 = states[1].spec["ItemProcessor"]["States"]["Step1"]
        assert substep2["Type"] == "Task"
        assert substep2["Resource"] == "arn:aws:states:::batch:submitJob.sync"
        assert substep2["End"] is True

        outputs2 = json.loads(states[2].spec["Parameters"]["outputs"])
        assert outputs2["output1"] == "outfile1.txt"
        assert outputs2["output2"] == "outfile2.txt"

    resource_gen = helper()
    resources = list(resource_gen)
    assert len(resources) == 1
    assert resources[0].name == "Step1JobDefx"
    assert resources[0].spec["Type"] == "Custom::BatchJobDefinition"
    subspec = json.loads(resources[0].spec["Properties"]["spec"])
    s3_tags = json.loads(subspec["parameters"]["s3tags"])
    expected_s3_tags = {
        "s3_tag1": "global_s3_value1",
        "s3_tag2": "step_s3_value2",
        "s3_tag3": "step_s3_value3",
    }
    assert s3_tags == expected_s3_tags

    spec = json.loads(resources[0].spec["Properties"]["spec"])
    cmd = json.loads(spec["parameters"]["command"])
    assert cmd[0] == "ls -l ${scatter.stuff} > ${output3}"


def test_handle_scatter_gather_too_deep():
    def helper():
        fake_step = Step("fake", {"fake": ""}, "fake_next_step")
        with pytest.raises(RuntimeError, match=r"Nested Scatter steps are not supported"):
            _ = yield from handle_scatter_gather(fake_step, {"wf": "params"}, 1)

    _ = list(helper())


def test_handle_scatter_gather_auto_inputs(sample_scatter_step, compiler_env):
    sample_scatter_step["inputs"] = None

    def helper():
        options = {"wf": "params", "s3_tags": {}, "job_tags": {}}
        test_step = Step("step_name", sample_scatter_step, "next_step_name")
        states = yield from handle_scatter_gather(test_step, options, 0)
        assert states[0].spec["Parameters"]["inputs.$"] == "States.JsonToString($.prev_outputs)"

    _ = dict(helper())

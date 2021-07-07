import json
import textwrap

import boto3
import moto
import pytest
import yaml

from ...src.compiler.pkg.batch_resources import parse_uri, get_ecr_uri, get_custom_job_queue_arn, get_job_queue,\
    get_memory_in_mibs, get_skip_behavior, batch_step, job_definition_rc, handle_batch, SCRATCH_PATH
from ...src.compiler.pkg.misc_resources import LAUNCHER_STACK_NAME
from ...src.compiler.pkg.util import CoreStack, Step, Resource, State


@pytest.mark.parametrize("uri, expected", [
    ("registry/path/image:version", ("registry/path", "image:version", "image", "version")),
    ("registry/path/image",         ("registry/path", "image", "image", None)),
    ("image:version",               (None, "image:version", "image", "version")),
    ("image",                       (None, "image", "image", None))
])
def test_parse_uri(uri, expected):
    response = parse_uri(uri)
    assert response == expected


@pytest.mark.parametrize("reg_v, expected", [
    ((None, "image:v1"), {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/image:v1"}),
    (("registry", "image:v1"), "registry/image:v1")
])
def test_get_ecr_uri(reg_v, expected):
    response = get_ecr_uri(*reg_v)
    assert response == expected


@pytest.mark.parametrize("req, mibs", [(10, 10), (1, 4), (9.1, 10), ("1G", 1024), ("9.1M", 10), ("1M", 4)])
def test_get_memory_in_mibs(req, mibs):
    result = get_memory_in_mibs(req)
    assert result == mibs


@pytest.fixture(scope="module")
def mock_custom_job_queue(aws_credentials):
    with moto.mock_iam():
        iam = boto3.resource("iam")
        role = iam.create_role(RoleName="test-role", AssumeRolePolicyDocument="{}")

        with moto.mock_batch():
            batch = boto3.client("batch")
            comp_env = batch.create_compute_environment(
                computeEnvironmentName="test-env",
                type="UNMANAGED",
                serviceRole=role.arn
            )

            queue = batch.create_job_queue(
                jobQueueName="custom-queue",
                state="ENABLED",
                priority=99,
                computeEnvironmentOrder=[
                    {
                        "computeEnvironment": comp_env["computeEnvironmentArn"],
                        "order": 1
                    }
                ]
            )

            yield queue


def test_get_custom_job_queue_arn(mock_custom_job_queue):
    result = get_custom_job_queue_arn("custom-queue")
    assert result == mock_custom_job_queue["jobQueueArn"]


@pytest.mark.parametrize("spec, expected", [
    ({"spot": True}, "spot_queue_arn"),
    ({"spot": False}, "on_demand_queue_arn"),
    ({"queue_name": "custom-queue"}, "arn:aws:batch:us-east-1:123456789012:job-queue/custom-queue")
])
def test_get_job_queue(spec, expected, monkeypatch, mock_core_stack, mock_custom_job_queue):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()
    result = get_job_queue(core_stack, spec)
    assert result == expected


@pytest.fixture(scope="module")
def sample_batch_step():
    ret = yaml.safe_load(textwrap.dedent("""
          commands: 
            - ${FASTP0200}/fastp --in1 ${reads1} --in2 ${reads2} --out1 ${paired1} --outdir ${outdir} --out2 ${paired2} --unpaired1 ${unpaired1} --unpaired2 ${unpaired2} --adapter_fasta ${adapter_file} --length_required 25 --json ${trim_log}
          compute:
            cpus: 4
            memory: 4 Gb
            spot: true
            type: memory
            gpu: 1
          image: skim3-fastp
          inputs: 
            adapter: ${adapter_path}${adapter_file}
            reads1: ${job.READ_PATH1}
            reads2: ${job.READ_PATH2}
          outputs: 
            paired1: paired_trim_1.fq
            paired2: paired_trim_2.fq
            unpaired1: unpaired_trim_1.fq
            unpaired2: unpaired_trim_2.fq
            trim_log: ${sample_id}-fastP.json
          references:
            reference1: s3://ref-bucket/path/to/reference.file
          params:
            outdir: outt
            sample_id: ${job.SAMPLE_ID}
            adapter_path: s3://bayer-skim-sequence-processing-696164428135/adapters/
            adapter_file: ${job.ADAPTER_FILE}
          qc_check: null
          skip_on_rerun: false
          timeout: 1h
          retry:
            attempts: 1
            interval: 1s
            backoff_rate: 1.0
      """))
    yield ret


@pytest.mark.parametrize("task_role", [
    "arn:task:role",
    {"Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.EcsTaskRoleArn"]},
])
def test_job_definition_rc(monkeypatch, mock_core_stack, task_role, sample_batch_step):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    step_name = "skim3-fastp"
    expected_job_def_name = f"Skim3FastpJobDef"

    expected_job_def = {
        "Type": "AWS::Batch::JobDefinition",
        "Properties": {
            "Type": "container",
            "Parameters": {
                "workflow_name": {"Ref": "AWS::StackName"},
                "repo": "rrr",
                "inputs": "iii",
                "references": "fff",
                "command": json.dumps(sample_batch_step["commands"]),
                "outputs": "ooo",
                "skip": "sss",
            },
            "ContainerProperties": {
                "Command": [
                    f"{SCRATCH_PATH}/select_runner.sh",
                    "--repo", "Ref::repo",
                    "--in", "Ref::inputs",
                    "--ref", "Ref::references",
                    "--cmd", "Ref::command",
                    "--out", "Ref::outputs",
                    "--skip", "Ref::skip",
                ],
                "Image": {
                    "Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/skim3-fastp",
                },
                "Environment": [
                    {"Name": "BC_WORKFLOW_NAME",   "Value": {"Ref": "AWS::StackName"}},
                    {"Name": "BC_SCRATCH_PATH",    "Value": SCRATCH_PATH},
                    {"Name": "BC_STEP_NAME",       "Value": step_name},
                    {"Name": "AWS_DEFAULT_REGION", "Value": {"Ref": "AWS::Region"}},
                ],
                "ResourceRequirements": [
                    {"Type": "VCPU",   "Value": "4"},
                    {"Type": "MEMORY", "Value": "4096"},
                    {"Type": "GPU",    "Value": "1"},
                ],
                "JobRoleArn": task_role,
                "MountPoints": [
                    {"ContainerPath": "/scratch",        "SourceVolume": "docker_scratch", "ReadOnly": False},
                    {"ContainerPath": "/_bclaw_scratch", "SourceVolume": "scratch",        "ReadOnly": False},
                ],
                "Volumes": [
                    {"Name": "docker_scratch", "Host": {"SourcePath": "/docker_scratch"}},
                    {"Name": "scratch",        "Host": {"SourcePath": "/scratch"}},
                ],
            },
            "Timeout": {
                "AttemptDurationSeconds": 3600,
            },
        },
    }

    def helper():
        step = Step(step_name, sample_batch_step, "next_step")
        job_def_name1 = yield from job_definition_rc(core_stack, step, task_role)
        assert job_def_name1 == expected_job_def_name

    for job_def_rc in helper():
        assert isinstance(job_def_rc, Resource)
        assert job_def_rc.name == expected_job_def_name
        assert job_def_rc.spec == expected_job_def


@pytest.mark.parametrize("spec, expect", [
    ({}, "none"),
    ({"skip_if_output_exists": True}, "output"),
    ({"skip_if_output_exists": False}, "none"),
    ({"skip_on_rerun": True}, "rerun"),
    ({"skip_on_rerun": False}, "none"),
])
def test_get_skip_behavior(spec, expect):
    result = get_skip_behavior(spec)
    assert result == expect


@pytest.mark.parametrize("next_step_name, next_or_end", [
    ("next_step", {"Next": "next_step"}),
    ("", {"End": True}),
])
def test_batch_step(next_step_name, next_or_end, monkeypatch, mock_core_stack, sample_batch_step):
    expected_body = {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Retry": [
            {
                "ErrorEquals": ["States.ALL"],
                "IntervalSeconds": 3,
                "MaxAttempts": 3,
                "BackoffRate": 1.5
            }
        ],
        "Parameters": {
            "JobName.$": "States.Format('{}__{}__{}__{}', $$.StateMachine.Name, $$.State.Name, $.id_prefix, $.index)",
            "JobDefinition": "${TestJobDef}",
            "JobQueue": "spot_queue_arn",
            "Parameters": {
                "repo.$": "$.repo",
                "references": json.dumps(sample_batch_step["references"]),
                "inputs": json.dumps(sample_batch_step["inputs"]),
                "outputs": json.dumps(sample_batch_step["outputs"]),
                "skip": "none",
            },
            "ContainerOverrides": {
                "Environment": [
                    {
                        "Name": "BC_BRANCH_IDX",
                        "Value.$": "$.index",
                    },
                    {
                        "Name": "BC_EXECUTION_ID",
                        "Value.$": "$$.Execution.Name",
                    },
                    {
                        "Name": "BC_LAUNCH_BUCKET",
                        "Value.$": "$.job_file.bucket"
                    },
                    {
                        "Name": "BC_LAUNCH_KEY",
                        "Value.$": "$.job_file.key",
                    },
                    {
                        "Name": "BC_LAUNCH_VERSION",
                        "Value.$": "$.job_file.version",
                    },
                    {
                        "Name": "BC_LAUNCH_S3_REQUEST_ID",
                        "Value.$": "$.job_file.s3_request_id",
                    },
                ],
            },
        },
        "ResultSelector": sample_batch_step["outputs"],
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **next_or_end
    }
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    spec = Step("step_name", sample_batch_step, next_step_name)
    result = batch_step(core_stack, spec, "TestJobDef")
    assert result == expected_body


@pytest.mark.parametrize("wf_params", [
    {"no_task_role": ""},
    {"task_role": "arn:from:workflow:params"}
])
@pytest.mark.parametrize("step_task_role_request", [
    {},
    {"task_role": "arn:from:step:spec"}
])
def test_handle_batch(wf_params, step_task_role_request, monkeypatch, mock_core_stack, sample_batch_step):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    if "task_role" in step_task_role_request:
        expected_job_role_arn = step_task_role_request["task_role"]
    elif "task_role" in wf_params:
        expected_job_role_arn = wf_params["task_role"]
    else:
        expected_job_role_arn = "ecs_task_role_arn"

    def helper():
        test_spec = {**sample_batch_step, **step_task_role_request}
        test_step = Step("step_name", test_spec, "next_step_name")
        states = yield from handle_batch(core_stack, test_step, wf_params)
        assert len(states) == 1
        assert isinstance(states[0], State)
        assert states[0].name == "step_name"
        assert states[0].spec["Resource"] == "arn:aws:states:::batch:submitJob.sync"
        assert states[0].spec["Parameters"]["JobDefinition"] == "${StepNameJobDef}"
        assert states[0].spec["Next"] == "next_step_name"

        references = json.loads(states[0].spec["Parameters"]["Parameters"]["references"])
        assert references["reference1"] == "s3://ref-bucket/path/to/reference.file"

        inputs = json.loads(states[0].spec["Parameters"]["Parameters"]["inputs"])
        assert inputs["adapter"] == "s3://bayer-skim-sequence-processing-696164428135/adapters/${job.ADAPTER_FILE}"

        outputs = json.loads(states[0].spec["Parameters"]["Parameters"]["outputs"])
        assert outputs["trim_log"] == "${job.SAMPLE_ID}-fastP.json"

    for resource in helper():
        assert isinstance(resource, Resource)
        assert resource.spec["Type"] == "AWS::Batch::JobDefinition"
        assert resource.spec["Properties"]["ContainerProperties"]["JobRoleArn"] == expected_job_role_arn
        assert " --outdir outt " in resource.spec["Properties"]["Parameters"]["command"]


def test_handle_batch_with_qc(monkeypatch, mock_core_stack, sample_batch_step):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    sample_batch_step["qc_check"] = {
        "qc_result_file": "qc_file.json",
        "stop_early_if": "test_expression",
        "email_subject": "test subject",
        "notification": [
            "test_one@case.com",
            "test_two@case.com",
        ],
    }

    def helper():
        step = Step("step_name", sample_batch_step, "next_step_name")
        states = yield from handle_batch(core_stack, step, {"wf": "params"})
        assert len(states) == 2
        assert all(isinstance(s, State) for s in states)

        assert states[0].name == "step_name"
        assert states[0].spec["Resource"] == "arn:aws:states:::batch:submitJob.sync"
        assert states[0].spec["Parameters"]["JobDefinition"] == "${StepNameJobDef}"
        assert states[0].spec["Next"] == "step_name.qc_checker"

        assert states[1].name == "step_name.qc_checker"
        assert states[1].spec["Next"] == "next_step_name"

    resource_gen = helper()
    resource_dict = dict(resource_gen)

    expected_keys = ["StepNameJobDef"]
    assert list(resource_dict.keys()) == expected_keys

    assert resource_dict["StepNameJobDef"]["Type"] == "AWS::Batch::JobDefinition"


def test_handle_batch_auto_inputs(monkeypatch, mock_core_stack, sample_batch_step):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    sample_batch_step["inputs"] = None

    def helper():
        step = Step("step_name", sample_batch_step, "next_step")
        states = yield from handle_batch(core_stack, step, {"wf": "params"})
        assert states[0].spec["Parameters"]["Parameters"]["inputs.$"] == "States.JsonToString($.prev_outputs)"

    _ = dict(helper())

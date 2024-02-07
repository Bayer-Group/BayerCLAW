import json
import textwrap

import pytest
import yaml

from ...src.compiler.pkg.batch_resources import expand_image_uri, get_job_queue, get_memory_in_mibs, \
    get_skip_behavior, get_environment, get_resource_requirements, get_volume_info, get_timeout, batch_step, \
    job_definition_rc, job_definition_rc1, handle_batch, SCRATCH_PATH
from ...src.compiler.pkg.util import Step, Resource, State


# Docker image tag format:
#   https://docs.docker.com/engine/reference/commandline/tag/#description
@pytest.mark.parametrize("uri, expected", [
    ("image", {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/image"}),
    ("image:ver", {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/image:ver"}),
    ("image:with.dots", {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/image:with.dots"}),
    ("registry/image", {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/registry/image"}),
    ("image:${tag}", {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/image:${!tag}"}),
    ("registry/image:tag", {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/registry/image:tag"}),
    ("level1/level2/${env}/image:${tag}", {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/level1/level2/${!env}/image:${!tag}"}),
    ("docker.io/library/ubuntu", "docker.io/library/ubuntu"),
    ("quay.io/biocontainers/edta:1.9.6--1", "quay.io/biocontainers/edta:1.9.6--1"),
    ("something.weird.com/really/deep/path/image:version", "something.weird.com/really/deep/path/image:version"),
    ("host-with-port:1234/image", "host-with-port:1234/image"),
])
def test_expand_image_uri(uri, expected):
    result = expand_image_uri(uri)
    assert result == expected


@pytest.mark.parametrize("req, mibs", [(10, 10), (1, 4), (9.1, 10), ("1G", 1024), ("9.1M", 10), ("1M", 4)])
def test_get_memory_in_mibs(req, mibs):
    result = get_memory_in_mibs(req)
    assert result == mibs


@pytest.mark.parametrize("spec, expected", [
    ({"spot": True}, "spot_queue_arn"),
    ({"spot": False}, "on_demand_queue_arn"),
    ({"spot": True, "queue_name": "custom-queue"}, "arn:aws:batch:${AWSRegion}:${AWSAccountId}:job-queue/custom-queue")
])
def test_get_job_queue(spec, expected, compiler_env):
    result = get_job_queue(spec)
    assert result == expected


def test_get_environment():
    step = Step("test_step", {}, "next_step")
    result = get_environment(step)
    expect = {
        "environment": [
            {"name": "BC_SCRATCH_PATH",
             "value": SCRATCH_PATH},
        ]
    }
    assert result == expect


@pytest.mark.parametrize("gpu", [0, 5, "all"])
def test_get_resource_requirements(gpu):
    spec = {
        "compute": {
            "cpus": 4,
            "memory": "4 Gb",
            "gpu": gpu,
        }
    }
    step = Step("test_step", spec, "next_step")
    result = get_resource_requirements(step)

    assert "resourceRequirements" in result
    rr = result["resourceRequirements"]
    assert isinstance(rr, list)

    assert rr[0] == {"type": "VCPU",
                     "value": "4"}
    assert rr[1] == {"type": "MEMORY",
                     "value": "4096"}
    if str(gpu) != "0":
        assert rr[2] == {"type": "GPU",
                         "value": str(gpu)}
        assert len(rr) == 3
    else:
        assert len(rr) == 2


@pytest.mark.parametrize("step_efs_specs", [
    [],
    [{"efs_id": "fs-12345", "host_path": "/efs1", "root_dir": "/"}],
    [{"efs_id": "fs-12345", "host_path": "/efs1", "root_dir": "/"},
     {"efs_id": "fs-98765", "host_path": "/efs2", "root_dir": "/path/to/files"}],
])
def test_get_volume_info(step_efs_specs):
    step = Step("test_step", {"filesystems": step_efs_specs}, "next_step")
    result = get_volume_info(step)
    assert "volumes" in result
    assert isinstance(result["volumes"], list)
    assert "mountPoints" in result
    assert isinstance(result["mountPoints"], list)
    v_mp = list(zip(result["volumes"], result["mountPoints"]))

    docker_socket_vol, docker_socket_mp = v_mp.pop(0)
    assert docker_socket_vol == {"name": "docker_socket",
                                 "host": {"sourcePath": "/var/run/docker.sock"}}
    assert docker_socket_mp == {"sourceVolume": "docker_socket",
                                "containerPath": "/var/run/docker.sock",
                                "readOnly": False,}

    scratch_vol, scratch_mp = v_mp.pop(0)
    assert scratch_vol == {"name": "scratch",
                           "host": {"sourcePath": "/scratch"},}
    assert scratch_mp == {"sourceVolume": scratch_vol["name"],
                          "containerPath": SCRATCH_PATH,
                          "readOnly": False,}

    docker_scratch_vol, docker_scratch_mp = v_mp.pop(0)
    assert docker_scratch_vol == {"name": "docker_scratch",
                                  "host": {"sourcePath": "/docker_scratch"},}
    assert docker_scratch_mp == {"sourceVolume": docker_scratch_vol["name"],
                                 "containerPath": "/.scratch",
                                 "readOnly": False}

    assert len(v_mp) == len(step_efs_specs)
    for ((vol, mp), spec) in zip(v_mp, step_efs_specs):
        assert vol == {"name": f"{spec['efs_id']}-volume",
                       "efsVolumeConfiguration": {
                           "fileSystemId": spec["efs_id"],
                           "rootDirectory": spec["root_dir"],
                           "transitEncryption": "ENABLED",
                       },}
        assert mp == {"sourceVolume": vol["name"],
                      "containerPath": spec["host_path"],
                      "readOnly": False,}


@pytest.mark.parametrize("timeout, expect", [
    (None, None),
    ("10 s", 60),
    ("100 s", 100),
    ("1 h", 3600)
])
def test_get_timeout(timeout, expect):
    step = Step("step_name", {"timeout": timeout}, "next_step")
    result = {"Properties": {"stuff": "yada yada", **get_timeout(step)}}
    if expect is None:
        assert "timeout" not in result["Properties"]
    else:
        assert "timeout" in result["Properties"]
        assert result["Properties"]["timeout"]["attemptDurationSeconds"] == expect


@pytest.fixture(scope="function")
def sample_batch_step():
    ret = yaml.safe_load(textwrap.dedent("""
          commands: 
            - >
             ${FASTP0200}/fastp 
             --in1 ${reads1}
             --in2 ${reads2}
             --outdir outt
             --out1 ${paired1}
             --out2 ${paired2} 
             --unpaired1 ${unpaired1} 
             --unpaired2 ${unpaired2}
             --adapter_fasta ${adapter}
             --length_required 25
             --json ${trim_log}
          compute:
            cpus: 4
            memory: 4 Gb
            spot: true
            type: memory
            gpu: 2
            shell: bash
          filesystems:
            -
              efs_id: fs-12345
              host_path: /step_efs
              root_dir: /path/to/my/data
          image: skim3-fastp
          inputs: 
            adapter: s3://bayer-skim-sequence-processing-696164428135/adapters/${job.ADAPTER_FILE}
            reads1: ${job.READ_PATH1}
            reads2: ${job.READ_PATH2}
          outputs: 
            paired1: paired_trim_1.fq
            paired2: paired_trim_2.fq
            unpaired1: unpaired_trim_1.fq
            unpaired2: unpaired_trim_2.fq
            trim_log: ${job.SAMPLE_ID}-fastP.json
          references:
            reference1: s3://ref-bucket/path/to/reference.file
          qc_check: null
          skip_on_rerun: false
          timeout: 1h
          retry:
            attempts: 1
            interval: 1s
            backoff_rate: 1.0
      """))
    return ret


# todo: remove
# @pytest.mark.skip(reason="may not need this anymore")
# @pytest.mark.parametrize("versioned", ["true", "false"])
# def test_job_definition_name(versioned):
#     if versioned == "true":
#         expect = {
#             "JobDefinitionName": {
#                 "Fn::Sub": [
#                     "${WFName}-${Step}--${Version}",
#                     {
#                         "WFName": {"Ref": "AWS::StackName"},
#                         "Step": "test_name",
#                         "Version": {"Fn::GetAtt": [LAUNCHER_STACK_NAME, "Outputs.LauncherLambdaVersion"]},
#                     }
#                 ]
#             }
#         }
#
#     else:
#         expect = {}
#
#     result = job_definition_name("test_name", versioned)
#     assert result == expect

def test_job_definition_rc1(sample_batch_step, compiler_env):
    step_name = "skim3-fastp"
    expected_rc_name = "Skim3FastpJobDef"
    step = Step(step_name, sample_batch_step, "next_step")

    expected_job_def_spec = {
        "type": "container",
        "parameters": {
            "repo": "rrr",
            "image": {
                "Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/skim3-fastp",
            },
            "inputs": "iii",
            "references": "fff",
            "command": json.dumps(step.spec["commands"]),
            "outputs": "ooo",
            "shell": "sh",
            "skip": "sss",
        },
        "containerProperties": {
            "image": "runner_repo_uri:1234567",
            "command": [
                "python", "/bclaw_runner/src/runner_cli.py",
                "--repo", "Ref::repo",
                "--image", "Ref::image",
                "--in", "Ref::inputs",
                "--ref", "Ref::references",
                "--cmd", "Ref::command",
                "--out", "Ref::outputs",
                "--shell", "Ref::shell",
                "--skip", "Ref::skip",
            ],
            "jobRoleArn": "arn:task:role",
            "environment": [
                {"name": "BC_SCRATCH_PATH", "value": SCRATCH_PATH},
            ],
            "resourceRequirements": [
                {"type": "VCPU", "value": "4"},
                {"type": "MEMORY", "value": "4096"},
                {"type": "GPU", "value": "2"},
            ],
            "mountPoints": [
                {"containerPath": "/var/run/docker.sock", "sourceVolume": "docker_socket", "readOnly": False},
                {"containerPath": "/_bclaw_scratch", "sourceVolume": "scratch", "readOnly": False},
                {"containerPath": "/.scratch", "sourceVolume": "docker_scratch", "readOnly": False},
                {"containerPath": "/step_efs", "sourceVolume": "fs-12345-volume", "readOnly": False},
            ],
            "volumes": [
                {"name": "docker_socket", "host": {"sourcePath": "/var/run/docker.sock"}},
                {"name": "scratch", "host": {"sourcePath": "/scratch"}},
                {"name": "docker_scratch", "host": {"sourcePath": "/docker_scratch"}},
                {"name": "fs-12345-volume",
                 "efsVolumeConfiguration": {
                     "fileSystemId": "fs-12345",
                     "rootDirectory": "/path/to/my/data",
                     "transitEncryption": "ENABLED",
                 }}
            ],
        },
        "schedulingPriority": 1,
        "timeout": {
            "attemptDurationSeconds": 3600,
        },
        "tags": {
            "bclaw:version": "1234567",
        }

    }

    expected_rc_spec = {
        "Type": "Custom::BatchJobDefinition",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "ServiceToken": "job_def_lambda_arn",
            "workflowName": {
                "Ref": "AWS::StackName",
            },
            "stepName": step_name,
            "spec": json.dumps(expected_job_def_spec, sort_keys=True),
        },
    }

    def helper():
        rc_name1 =  yield from job_definition_rc1(step, "arn:task:role", "sh")
        assert rc_name1 == expected_rc_name

    for resource in helper():
        assert isinstance(resource, Resource)
        assert resource.name == expected_rc_name
        assert resource.spec == expected_rc_spec

@pytest.mark.skip(reason="remove me")
def test_job_definition_rc(sample_batch_step, compiler_env):
    step_name = "skim3-fastp"
    expected_job_def_name = f"Skim3FastpJobDef"

    step = Step(step_name, sample_batch_step, "next_step")

    expected_job_def = {
        "Type": "AWS::Batch::JobDefinition",
        "UpdateReplacePolicy": "Retain",
        "Properties": {
            "Type": "container",
            "Parameters": {
                "workflow_name": {"Ref": "AWS::StackName"},
                "repo": "rrr",
                "image": {
                    "Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/skim3-fastp",
                },
                "inputs": "iii",
                "references": "fff",
                "command": json.dumps(step.spec["commands"]),
                "outputs": "ooo",
                "shell": "bash",
                "skip": "sss",
            },
            "ContainerProperties": {
                "Command": [
                    "python", "/bclaw_runner/src/runner_cli.py",
                    "--repo", "Ref::repo",
                    "--image", "Ref::image",
                    "--in", "Ref::inputs",
                    "--ref", "Ref::references",
                    "--cmd", "Ref::command",
                    "--out", "Ref::outputs",
                    "--shell", "Ref::shell",
                    "--skip", "Ref::skip",
                ],
                "Image": "runner_repo_uri:1234567",
                "Environment": [
                    {"Name": "BC_WORKFLOW_NAME",   "Value": {"Ref": "AWS::StackName"}},
                    {"Name": "BC_SCRATCH_PATH",    "Value": SCRATCH_PATH},
                    {"Name": "BC_STEP_NAME",       "Value": step_name},
                    {"Name": "AWS_DEFAULT_REGION", "Value": {"Ref": "AWS::Region"}},
                    {"Name": "AWS_ACCOUNT_ID",     "Value": {"Ref": "AWS::AccountId"}},
                ],
                "ResourceRequirements": [
                    {"Type": "VCPU",   "Value": "4"},
                    {"Type": "MEMORY", "Value": "4096"},
                    {"Type": "GPU",    "Value": "2"},
                ],
                "JobRoleArn": "arn:task:role",
                "MountPoints": [
                    {"ContainerPath": "/var/run/docker.sock", "SourceVolume": "docker_socket",   "ReadOnly": False},
                    {"ContainerPath": "/_bclaw_scratch",      "SourceVolume": "scratch",         "ReadOnly": False},
                    {"ContainerPath": "/.scratch",            "SourceVolume": "docker_scratch",  "ReadOnly": False},
                    {"ContainerPath": "/step_efs",            "SourceVolume": "fs-12345-volume", "ReadOnly": False},
                ],
                "Volumes": [
                    {"Name": "docker_socket",  "Host": {"SourcePath": "/var/run/docker.sock"}},
                    {"Name": "scratch",        "Host": {"SourcePath": "/scratch"}},
                    {"Name": "docker_scratch", "Host": {"SourcePath": "/docker_scratch"}},
                    {"Name": "fs-12345-volume",
                     "EfsVolumeConfiguration": {
                        "FileSystemId":      "fs-12345",
                        "RootDirectory":     "/path/to/my/data",
                        "TransitEncryption": "ENABLED",
                     }}
                ],
            },
            "SchedulingPriority": 1,
            "Timeout": {
                "AttemptDurationSeconds": 3600,
            },
            "Tags": {
                "bclaw:version": "1234567",
            }
        },
    }

    def helper():
        job_def_name1 = yield from job_definition_rc(step, "arn:task:role", "bash")
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


@pytest.mark.parametrize("scattered, job_name", [
    (True, "States.Format('{}__{}__{}', $$.Execution.Name, $$.State.Name, $.index)"),
    (False, "States.Format('{}__{}', $$.Execution.Name, $$.State.Name)")
])
@pytest.mark.parametrize("next_step_name, next_or_end", [
    ("next_step", {"Next": "next_step"}),
    ("", {"End": True}),
])
def test_batch_step(next_step_name, next_or_end, sample_batch_step, scattered, job_name, compiler_env):
    step = Step("step_name", sample_batch_step, next_step_name)

    expected_body = {
        "Type": "Task",
        "Resource": "arn:aws:states:::batch:submitJob.sync",
        "Retry": [
            {
                "ErrorEquals": ["Batch.AWSBatchException"],
                "IntervalSeconds": 30,
                "MaxAttempts": 20,
                "MaxDelaySeconds": 300,
                "BackoffRate": 2.0,
                "JitterStrategy": "FULL",
            },
            {
                "ErrorEquals": ["States.ALL"],
                "IntervalSeconds": 3,
                "MaxAttempts": 3,
                "BackoffRate": 1.5
            },
        ],
        "Parameters": {
            "JobName.$": job_name,
            "JobDefinition": "${TestJobDef}",
            "JobQueue": "spot_queue_arn",
            "ShareIdentifier.$": "$.share_id",
            "Parameters": {
                "repo.$": "$.repo.uri",
                # "image": {
                #     "Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/skim3-fastp",
                # },
                "references": json.dumps(step.spec["references"]),
                "inputs": json.dumps(step.spec["inputs"]),
                # "command": json.dumps(step.spec["commands"]),
                "outputs": json.dumps(step.spec["outputs"]),
                # "shell": "sh",
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
                ],
            },
            # "Timeout": {
            #     "attemptDurationSeconds": 3600,
            # },
        },
        "ResultSelector": step.spec["outputs"],
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **next_or_end
    }

    result = batch_step(step, "TestJobDef", scattered)
    assert result == expected_body


@pytest.mark.parametrize("options", [
    {"no_task_role": "", "versioned": "true"},
    {"task_role": "arn:from:workflow:params", "versioned": "true"}
])
@pytest.mark.parametrize("step_task_role_request", [
    {},
    {"task_role": "arn:from:step:spec"}
])
def test_handle_batch(options, step_task_role_request, sample_batch_step, compiler_env):
    if "task_role" in step_task_role_request:
        expected_job_role_arn = step_task_role_request["task_role"]
    elif "task_role" in options:
        expected_job_role_arn = options["task_role"]
    else:
        expected_job_role_arn = "ecs_task_role_arn"

    def helper():
        test_spec = {**sample_batch_step, **step_task_role_request}
        test_step = Step("step_name", test_spec, "next_step_name")
        states = yield from handle_batch(test_step, options, False)
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
        assert resource.spec["Type"] == "Custom::BatchJobDefinition"

        job_def_spec = json.loads(resource.spec["Properties"]["spec"])
        assert job_def_spec["containerProperties"]["jobRoleArn"] == expected_job_role_arn
        # assert " --outdir outt " in job_def_spec["parameters"]["command"]  # moved to state machine


def test_handle_batch_with_qc(sample_batch_step, compiler_env):
    step = Step("step_name", sample_batch_step, "next_step_name")

    step.spec["qc_check"] = {
        "qc_result_file": "qc_file.json",
        "stop_early_if": "test_expression",
        "email_subject": "test subject",
        "notification": [
            "test_one@case.com",
            "test_two@case.com",
        ],
    }

    def helper():
        states = yield from handle_batch(step, {"wf": "params", "versioned": "true"}, False)
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

    assert resource_dict["StepNameJobDef"]["Type"] == "Custom::BatchJobDefinition"


def test_handle_batch_auto_inputs(sample_batch_step, compiler_env):
    step = Step("step_name", sample_batch_step, "next_step")
    step.spec["inputs"] = None

    def helper():
        states = yield from handle_batch(step, {"wf": "params", "versioned": "true"}, False)
        assert states[0].spec["Parameters"]["Parameters"]["inputs.$"] == "States.JsonToString($.prev_outputs)"

    _ = dict(helper())


@pytest.mark.parametrize("step_shell, expect", [
    (None, "sh"),
    ("bash", "bash"),
])
def test_handle_batch_shell_opt(sample_batch_step, step_shell, expect, compiler_env):
    step = Step("step_name", sample_batch_step, "next_step")
    step.spec["compute"]["shell"] = step_shell

    def helper():
        _ = yield from handle_batch(step, {"shell": "sh", "versioned": "true"}, False)
        # assert states[0].spec["Parameters"]["Parameters"]["shell"] == expect

    # _ = dict(helper())
    rc = dict(helper())

    spec = json.loads(rc["StepNameJobDef"]["Properties"]["spec"])
    assert spec["parameters"]["shell"] == expect
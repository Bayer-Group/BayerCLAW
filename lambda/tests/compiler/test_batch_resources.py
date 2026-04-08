import json
import textwrap

import boto3
import moto
import pytest
import yaml

from ...src.compiler.pkg.batch_resources import (expand_image_uri, get_job_queue, get_memory_in_mibs,
    get_skip_behavior, get_environment, get_resource_requirements, get_volume_info, get_timeout, handle_qc_check,
    get_consumable_resource_properties, get_output_uris, batch_step, job_definition_rc, handle_batch, SCRATCH_PATH)
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
    image_spec = {"name": uri, "auth": "doesnt_change"}
    result = expand_image_uri(image_spec)
    assert result["name"] == expected
    assert result["auth"] == "doesnt_change"

@pytest.mark.parametrize("req, mibs", [(10, 10), (1, 4), (9.1, 10), ("1G", 1024), ("9.1M", 10), ("1M", 4)])
def test_get_memory_in_mibs(req, mibs):
    result = get_memory_in_mibs(req)
    assert result == mibs


@pytest.mark.parametrize("spec, expected", [
    ({"spot": True, "gpu": 0}, "spot_queue_arn"),
    ({"spot": True, "gpu": 99}, "spot_gpu_queue_arn"),
    ({"spot": True, "gpu": "all"}, "spot_gpu_queue_arn"),
    ({"spot": False, "gpu": 0}, "on_demand_queue_arn"),
    ({"spot": False, "gpu": 88}, "on_demand_gpu_queue_arn"),
    ({"spot": False, "gpu": "all"}, "on_demand_gpu_queue_arn"),
    ({"spot": True,  "gpu": 0, "queue_name": "custom-queue"}, "arn:aws:batch:${AWSRegion}:${AWSAccountId}:job-queue/custom-queue"),
    ({"spot": True,  "gpu": 77, "queue_name": "custom-queue"}, "arn:aws:batch:${AWSRegion}:${AWSAccountId}:job-queue/custom-queue"),
    ({"spot": True,  "gpu": "all", "queue_name": "custom-queue"}, "arn:aws:batch:${AWSRegion}:${AWSAccountId}:job-queue/custom-queue"),
])
def test_get_job_queue(spec, expected, compiler_env):
    result = get_job_queue(spec)
    assert result == expected


def test_get_environment():
    step = Step("test_step", {}, "next_step")
    result = get_environment(step)
    expect = {
        "Environment": [
            {"Name": "BC_WORKFLOW_NAME",
             "Value": {"Ref": "AWS::StackName"}},
            {"Name": "BC_SCRATCH_PATH",
             "Value": SCRATCH_PATH},
            {"Name": "BC_STEP_NAME",
             "Value": "test_step"},
            {"Name": "AWS_DEFAULT_REGION",
             "Value": {"Ref": "AWS::Region"}},
            {"Name": "AWS_ACCOUNT_ID",
             "Value": {"Ref": "AWS::AccountId"}},
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

    assert "ResourceRequirements" in result
    rr = result["ResourceRequirements"]
    assert isinstance(rr, list)

    assert rr[0] == {"Type": "VCPU",
                     "Value": "4"}
    assert rr[1] == {"Type": "MEMORY",
                     "Value": "4096"}
    if str(gpu) != "0":
        assert rr[2] == {"Type": "GPU",
                         "Value": str(gpu)}
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
    assert "Volumes" in result
    assert isinstance(result["Volumes"], list)
    assert "MountPoints" in result
    assert isinstance(result["MountPoints"], list)
    v_mp = list(zip(result["Volumes"], result["MountPoints"]))

    docker_socket_vol, docker_socket_mp = v_mp.pop(0)
    assert docker_socket_vol == {"Name": "docker_socket",
                                 "Host": {"SourcePath": "/var/run/docker.sock"}}
    assert docker_socket_mp == {"SourceVolume": "docker_socket",
                                "ContainerPath": "/var/run/docker.sock",
                                "ReadOnly": False,}

    scratch_vol, scratch_mp = v_mp.pop(0)
    assert scratch_vol == {"Name": "scratch",
                           "Host": {"SourcePath": "/scratch"},}
    assert scratch_mp == {"SourceVolume": scratch_vol["Name"],
                          "ContainerPath": SCRATCH_PATH,
                          "ReadOnly": False,}

    docker_scratch_vol, docker_scratch_mp = v_mp.pop(0)
    assert docker_scratch_vol == {"Name": "docker_scratch",
                                  "Host": {"SourcePath": "/docker_scratch"},}
    assert docker_scratch_mp == {"SourceVolume": docker_scratch_vol["Name"],
                                 "ContainerPath": "/.scratch",
                                 "ReadOnly": False}

    assert len(v_mp) == len(step_efs_specs)
    for ((vol, mp), spec) in zip(v_mp, step_efs_specs):
        assert vol == {"Name": f"{spec['efs_id']}-volume",
                       "EfsVolumeConfiguration": {
                           "FileSystemId": spec["efs_id"],
                           "RootDirectory": spec["root_dir"],
                           "TransitEncryption": "ENABLED",
                       },}
        assert mp == {"SourceVolume": vol["Name"],
                      "ContainerPath": spec["host_path"],
                      "ReadOnly": False,}


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
        assert "Timeout" not in result["Properties"]
    else:
        assert "Timeout" in result["Properties"]
        assert result["Properties"]["Timeout"]["AttemptDurationSeconds"] == expect


@pytest.mark.parametrize("qc_spec, expect", [
    (None, []),
    ({"qc_result_file": "qc.out", "stop_early_if": "x == 1"}, [{"qc_result_file": "qc.out", "stop_early_if": ["x == 1"]}]),
    ({"qc_result_file": "qc.out", "stop_early_if": ["x == 1", "y == 2"]}, [{"qc_result_file": "qc.out", "stop_early_if": ["x == 1", "y == 2"]}]),
    ([{"qc_result_file": "qc1.out", "stop_early_if": ["x == 1", "y == 2"]},
      {"qc_result_file": "qc2.out", "stop_early_if": "z == 3"}],
     [{"qc_result_file": "qc1.out", "stop_early_if": ["x == 1", "y == 2"]},
      {"qc_result_file": "qc2.out", "stop_early_if": ["z == 3"]}]),
])
def test_handle_qc_check(qc_spec, expect):
    result = handle_qc_check(qc_spec)
    assert result == expect


@pytest.mark.parametrize("spec, expect", [
    ({"res1": 99, "res2": 98},
     {
         "ConsumableResourceProperties": {
             "ConsumableResourceList": [
                {"ConsumableResource": "res1", "Quantity": 99},
                {"ConsumableResource": "res2", "Quantity": 98},
             ],
         },
    }),
    ({}, {}),
])
def test_get_consumable_resource_properties(spec, expect):
    result = get_consumable_resource_properties(spec)
    assert result == expect


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
            consumes:
              "resource1": 99
              "resource2": 88

          job_tags:
            job_tag2: step_job_value2
            job_tag3: step_job_value3
          filesystems:
            -
              efs_id: fs-12345
              host_path: /step_efs
              root_dir: /path/to/my/data
          image:
            name: skim3-fastp
            auth: arn:aws:secretsmanager:us-west-1:123456789012:secret:docker_auth
          inputs:
            adapter: s3://bayer-skim-sequence-processing-696164428135/adapters/${job.ADAPTER_FILE}
            reads1: ${job.READ_PATH1}
            reads2: ${job.READ_PATH2}
          outputs: 
            paired1:
                name: paired_trim_1.fq
                s3_tags:
                    tag3: paired_trim_1_value3
                    tag4: paired_trim_1_value4
            paired2:
                name: paired_trim_2.fq
                s3_tags:
                    tag3: paired_trim_2_value3
                    tag4: paired_trim_2_value4
            unpaired1:
                name: unpaired_trim_1.fq
                dest: s3://bucket/path/to/
                s3_tags:
                    tag3: unpaired_trim1_value3
                    tag4: unpaired_trim_1_value4
            unpaired2:
                name: unpaired_trim_2.fq
                dest: s3://bucket/path/to/
                s3_tags:
                    tag3: unpaired_trim2_value3
                    tag4: unpaired_trim_2_value4
            trim_log:
                name: ${job.SAMPLE_ID}-fastP.json
                dest: s3://bucket/path/to/
                s3_tags: {}
          references:
            reference1: s3://ref-bucket/path/to/reference.file
          s3_tags:
            "tag2": "step_s3_value2"
            "tag3": "step_s3_value3"
          qc_check:
            -
                qc_result_file: qc.out
                stop_early_if:
                    - x > 1
          skip_on_rerun: false
          timeout: 1h
          retry:
            attempts: 1
            interval: 1s
            backoff_rate: 1.0
      """))
    return ret


def test_job_definition_rc(sample_batch_step, compiler_env):
    step_name = "skim3-fastp"
    expected_rc_name = "Skim3FastpJobDefz"
    step = Step(step_name, sample_batch_step, "next_step")

    s3_tags = {
        "s3_tag1": "global_s3_value1",
        "s3_tag2": "step_s3_value2",
    }

    job_tags = {
        "job_tag1": "global_job_value1",
        "job_tag2": "step_job_value2",
    }

    image_spec = {
        "auth": "arn:aws:secretsmanager:us-west-1:123456789012:secret:docker_auth",
        "name": {"Fn::Sub": "${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/skim3-fastp"},
    }

    properties_spec = {
        "Type": "container",
        "Parameters": {
            "repo": "rrr",
            "image": json.dumps(image_spec, sort_keys=True, separators=(",", ":")),
            "inputs": "iii",
            "references": "fff",
            "command": json.dumps(step.spec["commands"], separators=(",", ":")),
            "outputs": "ooo",
            "qc": json.dumps(step.spec["qc_check"], separators=(",", ":")),
            "shell": "sh",
            "skip": "sss",
            "s3tags": json.dumps(s3_tags, separators=(",", ":")),
        },
        "ContainerProperties": {
            "Image": "runner_repo_uri:1234567",
            "Command": [
                "python", "/bclaw_runner/src/runner_cli.py",
                "-c", "Ref::command",
                "-f", "Ref::references",
                "-i", "Ref::inputs",
                "-k", "Ref::skip",
                "-m", "Ref::image",
                "-o", "Ref::outputs",
                "-q", "Ref::qc",
                "-r", "Ref::repo",
                "-s", "Ref::shell",
                "-t", "Ref::s3tags",
            ],
            "JobRoleArn": "arn:task:role",
            "Environment": [
                {"Name": "BC_WORKFLOW_NAME", "Value": {"Ref": "AWS::StackName"}},
                {"Name": "BC_SCRATCH_PATH", "Value": SCRATCH_PATH},
                {"Name": "BC_STEP_NAME", "Value": step_name},
                {"Name": "AWS_DEFAULT_REGION", "Value": {"Ref": "AWS::Region"}},
                {"Name": "AWS_ACCOUNT_ID", "Value": {"Ref": "AWS::AccountId"}},
            ],
            "ResourceRequirements": [
                {"Type": "VCPU", "Value": "4"},
                {"Type": "MEMORY", "Value": "4096"},
                {"Type": "GPU", "Value": "2"},
            ],
            "MountPoints": [
                {"ContainerPath": "/var/run/docker.sock", "SourceVolume": "docker_socket", "ReadOnly": False},
                {"ContainerPath": "/_bclaw_scratch", "SourceVolume": "scratch", "ReadOnly": False},
                {"ContainerPath": "/.scratch", "SourceVolume": "docker_scratch", "ReadOnly": False},
                {"ContainerPath": "/step_efs", "SourceVolume": "fs-12345-volume", "ReadOnly": False},
            ],
            "Volumes": [
                {"Name": "docker_socket", "Host": {"SourcePath": "/var/run/docker.sock"}},
                {"Name": "scratch", "Host": {"SourcePath": "/scratch"}},
                {"Name": "docker_scratch", "Host": {"SourcePath": "/docker_scratch"}},
                {"Name": "fs-12345-volume",
                 "EfsVolumeConfiguration": {
                     "FileSystemId": "fs-12345",
                     "RootDirectory": "/path/to/my/data",
                     "TransitEncryption": "ENABLED",
                 }}
            ],
        },
        "ConsumableResourceProperties": {
            "ConsumableResourceList": [
                {
                    "ConsumableResource": "resource1",
                    "Quantity": 99,
                },
                {
                    "ConsumableResource": "resource2",
                    "Quantity": 88,
                },
            ],
        },
        "SchedulingPriority": 1,
        "Timeout": {
            "AttemptDurationSeconds": 3600,
        },
        "ResourceRetentionPolicy": {
            "SkipDeregisterOnUpdate": True,
        },
        "PropagateTags": True,
        "Tags": {
            "job_tag1": "global_job_value1",
            "job_tag2": "step_job_value2",
            "bclaw:step": step_name,
            "bclaw:version": "1234567",
            "bclaw:workflow": {"Ref": "AWS::StackName"},
        }
    }

    expected_rc_spec = {
        "Type": "AWS::Batch::JobDefinition",
        "UpdateReplacePolicy": "Retain",
        "Properties": properties_spec,
    }

    def helper():
        rc_name1 = yield from job_definition_rc(step, "arn:task:role", "sh", s3_tags, job_tags)
        assert rc_name1 == expected_rc_name

    for resource in helper():
        assert isinstance(resource, Resource)
        assert resource.name == expected_rc_name
        assert resource.spec == expected_rc_spec


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


def test_get_output_uris():
    out_spec = {
        "one": {
            "name": "one.txt"
        },
        "two": {
            "name": "two.txt",
            "dest": "s3://bucket/path/to/",
        },
    }

    expect = {
        "one": "one.txt",
        "two": "s3://bucket/path/to/two.txt",
    }

    result = get_output_uris(out_spec)
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
            "JobQueue": "spot_gpu_queue_arn",
            "ShareIdentifier.$": "$.share_id",
            "Parameters": {
                "repo.$": "$.repo.uri",
                "references": json.dumps(step.spec["references"], separators=(",", ":")),
                "inputs": json.dumps(step.spec["inputs"], separators=(",", ":")),
                "outputs": json.dumps(step.spec["outputs"], separators=(",", ":")),
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
            "Tags": {
                "bclaw:jobfile.$": "$.job_file.key"
            }
        },
        "ResultSelector": {
            "paired1": "paired_trim_1.fq",
            "paired2": "paired_trim_2.fq",
            "unpaired1": "s3://bucket/path/to/unpaired_trim_1.fq",
            "unpaired2": "s3://bucket/path/to/unpaired_trim_2.fq",
            "trim_log": "s3://bucket/path/to/${job.SAMPLE_ID}-fastP.json",
        },
        "ResultPath": "$.prev_outputs",
        "OutputPath": "$",
        **next_or_end
    }

    result = batch_step(step, "TestJobDef", scattered)
    assert result == expected_body


@pytest.mark.parametrize("options", [
    {"no_task_role": "", "s3_tags": {}, "job_tags": {}},
    {"task_role": "arn:from:workflow:params", "s3_tags": {}, "job_tags": {}}
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
        assert states[0].spec["Parameters"]["JobDefinition"] == "${StepNameJobDefz}"
        assert states[0].spec["Next"] == "next_step_name"

        references = json.loads(states[0].spec["Parameters"]["Parameters"]["references"])
        assert references["reference1"] == "s3://ref-bucket/path/to/reference.file"

        inputs = json.loads(states[0].spec["Parameters"]["Parameters"]["inputs"])
        assert inputs["adapter"] == "s3://bayer-skim-sequence-processing-696164428135/adapters/${job.ADAPTER_FILE}"

        outputs = json.loads(states[0].spec["Parameters"]["Parameters"]["outputs"])
        assert outputs["trim_log"]["name"] == "${job.SAMPLE_ID}-fastP.json"

    for resource in helper():
        assert isinstance(resource, Resource)
        assert resource.spec["Type"] == "AWS::Batch::JobDefinition"

        job_role_arn = resource.spec["Properties"]["ContainerProperties"]["JobRoleArn"]
        assert job_role_arn == expected_job_role_arn


def test_handle_batch_auto_inputs(sample_batch_step, compiler_env):
    step = Step("step_name", sample_batch_step, "next_step")
    step.spec["inputs"] = None

    def helper():
        options = {"wf": "params", "s3_tags": {}, "job_tags": {}}
        states = yield from handle_batch(step, options, False)
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
        options = {"shell": "sh", "s3_tags": {}, "job_tags": {}}
        _ = yield from handle_batch(step, options, False)

    rc = dict(helper())

    spec = rc["StepNameJobDefz"]["Properties"]["Parameters"]["shell"]
    assert spec == expect


def test_handle_batch_s3_tags_opt(sample_batch_step, compiler_env):
    global_s3_tags = {
        "tag1": "global_s3_value1",
        "tag2": "global_s3_value2",
    }

    expected_s3_tags = {
        "tag1": "global_s3_value1",
        "tag2": "step_s3_value2",
        "tag3": "step_s3_value3",
    }

    step = Step("step_name", sample_batch_step, "next_step")

    def helper():
        options = {"wf": "params", "versioned": "true", "s3_tags": global_s3_tags, "job_tags": {}}
        _ = yield from handle_batch(step, options, False)

    rc = dict(helper())

    tags = json.loads(rc["StepNameJobDefz"]["Properties"]["Parameters"]["s3tags"])
    assert tags == expected_s3_tags


def test_handle_batch_job_tags_opt(sample_batch_step, compiler_env):
    global_job_tags = {
        "job_tag1": "global_job_value1",
        "job_tag2": "global_job_value2",
    }

    expected_job_tags = {
        "job_tag1": "global_job_value1",
        "job_tag2": "step_job_value2",
        "job_tag3": "step_job_value3",
        "bclaw:step": "step_name",
        "bclaw:version": "1234567",
        "bclaw:workflow": {"Ref": "AWS::StackName"},
    }

    step = Step("step_name", sample_batch_step, "next_step")

    def helper():
        options = {"wf": "params", "versioned": "true", "s3_tags": {}, "job_tags": global_job_tags}
        _ = yield from handle_batch(step, options, False)

    rc = dict(helper())

    tags = rc["StepNameJobDefz"]["Properties"]["Tags"]
    assert tags == expected_job_tags

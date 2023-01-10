import json
import pytest

from ...src.compiler.pkg.util import CoreStack, Step, make_logical_name, _param_subber, \
    do_param_substitution, time_string_to_seconds, merge_params_and_options


@pytest.mark.parametrize("next_step, expect", [
    ("", True),
    ("not_terminal", False)
])
def test_step_is_terminal(next_step, expect):
    step = Step("name", {}, next_step)
    result = step.is_terminal
    assert result == expect


@pytest.mark.parametrize("step, expect", [
    (Step("name1", {"Other": "stuff"}, "next_step"), {"Next": "next_step"}),
    (Step("name2", {"Other": "stuff"}, ""), {"End": True}),
])
def test_step_next_or_end(step, expect):
    result = step.next_or_end
    assert result == expect


@pytest.mark.parametrize("step, expect", [
    (Step("name1", {"Other": "stuff", "inputs": {"file1": "one", "file2": "two"}}, ""), {"inputs": json.dumps({"file1": "one", "file2": "two"})}),
    (Step("name2", {"Other": "stuff", "inputs": {}}, ""), {"inputs": json.dumps({})}),
    (Step("name3", {"Other": "stuff", "inputs": None}, ""), {"inputs.$": "States.JsonToString($.prev_outputs)"})
])
def test_step_input_field(step, expect):
    result = step.input_field
    assert result == expect


@pytest.mark.skip(reason="going away")
def test_core_stack_output(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()
    result = core_stack.output("SpotQueueArn")
    assert result == "spot_queue_arn"


def test_make_logical_name():
    orig_name = "a-name  with++LOTS___of%wEiRd,,\n,,characters/that~will&NEVER(work)as\ta##LOGICAL!name12345"
    result = make_logical_name(orig_name)
    expect = "ANameWithLotsOfWeirdCharactersThatWillNeverWorkAsALogicalName12345"
    assert result == expect


@pytest.mark.parametrize("target, expect", [
    ("${value1} ${value2} ${dotted.value3} ${skip_me}",
     "string ${reference} 42 ${skip_me}"),
    (["${value1}", "${value2}", "${dotted.value3}", "${skip_me}"],
     ["string", "${reference}", "42", "${skip_me}"]),
    ({"k1": "${value1}", "k2": "${value2}", "k3": "${dotted.value3}", "k4": "${skip_me}"},
     {"k1": "string", "k2": "${reference}", "k3": "42", "k4": "${skip_me}"}),
    (99, 99)
])
def test_param_subber(target, expect):
    params = {
        "value1": "string",
        "value2": "${reference}",
        "dotted.value3": 42,
        "value4": "not used",
    }
    result = _param_subber(params, target)
    assert result == expect


def test_param_subber_empty_params():
    params = {}
    target = "${one} ${two} ${three}"
    result = _param_subber(params, target)
    assert result == target


def test_do_param_substitution_batch():
    spec = {
        "image": "test-image",
        "task_role": "arn:task:role",
        "params": {
            "param1": "string",
            "param2": "${job.field}",
            "param3": 42
        },
        "inputs": {
            "input1": "${param1}.txt",
            "input2": "in_${param2}.cfg",
            "input3": "${param3}_things.lst",
        },
        "commands": [
            "do_something ${input1} ${param1} > ${output1}",
            "do_something_else ${input2} ${ENVIRONMENT_VAR} ${param2} > ${output2}",
            "do_one_mort_thing ${input2} ${input3} ${param3} ${param3} > ${output3} 2> qc.json",
        ],
        "qc_check": {
            "qc_result_file": "qc.json",
            "stop_early_if": "float(something) > 0.999",
        },
        "outputs": {
            "output1": "${param1}_${job.whatever}_out.txt",
            "output2": "run_${param2}_${param3}.log",
            "output3": "${param3}_${param2}_${what_is_this}_${param1}.out",
            "qc_file": "qc.json",
        },
        "skip_if_output_exists": True,
        "compute": {
            "cpus": 99,
            "memory": 1024,
            "spot": True,
            "queue_name": "custom",
        },
    }
    result = do_param_substitution(spec)
    expect = {
        "image": "test-image",
        "task_role": "arn:task:role",
        "params": {},
        "inputs": {
            "input1": "string.txt",
            "input2": "in_${job.field}.cfg",
            "input3": "42_things.lst",
        },
        "commands": [
            "do_something ${input1} string > ${output1}",
            "do_something_else ${input2} ${ENVIRONMENT_VAR} ${job.field} > ${output2}",
            "do_one_mort_thing ${input2} ${input3} 42 42 > ${output3} 2> qc.json",
        ],
        "qc_check": {
            "qc_result_file": "qc.json",
            "stop_early_if": "float(something) > 0.999",
        },
        "outputs": {
            "output1": "string_${job.whatever}_out.txt",
            "output2": "run_${job.field}_42.log",
            "output3": "42_${job.field}_${what_is_this}_string.out",
            "qc_file": "qc.json",
        },
        "skip_if_output_exists": True,
        "compute": {
            "cpus": 99,
            "memory": 1024,
            "spot": True,
            "queue_name": "custom",
        },

    }
    assert result == expect


def test_do_param_substitution_scatter():
    # please don't write workflows like this
    spec = {
        "scatter": {
            "stuff": "file*.txt"
        },
        "params": {
            "param1": "string",
            "param2": "${job.field}",
            "param3": 42
        },
        "inputs": {
            "input1": "${param1}.txt",
            "input2": "in_${job.field}.cfg",
            "input3": "${param3}_things.lst",
        },
        "steps": [
            {
                "Step1": {
                    "image": "test-image",
                    "params": {
                        "param1": "step1_${parent.param1}",
                        "param2": "step1_${parent.param2}",
                    },
                    "inputs": {
                        "input1": "step1_input1_${parent.param1}.txt",
                        "input2": "step1_input2_${parent.param2}_${parent.param3}.lst",
                    },
                    "commands": [
                        "do_something ${scatter.stuff} ${parent.param1} ${param1} ${parent.input1} > ${output1}",
                        "do_something_else ${parent.input2} ${parent.param2} ${scatter.stuff} ${param2} > ${output2}",
                    ],
                    "outputs": {
                        "output1": "step1_output1_${parent.param2}.out",
                        "output2": "step1_output2_${parent.param3}.log",
                    },
                    "skip_if_output_exists": True,
                    "compute": {
                        "cpus": 99,
                        "memory": 1024,
                        "spot": True,
                        "queue_name": "custom",
                    },
                },
                "Step2": {
                    "image": "test-image",
                    "params": {
                        "param1": "${parent.param2}_step2",
                        "param2": "${parent.param3}",
                    },
                    "inputs": {
                        "input1": "${parent.param3}_step2_input1.xyz",
                        "input2": "${parent.param1}.abc",
                    },
                    "commands": [
                        "do_more_stuff ${parent.param1} ${param2} ${parent.input3} > ${output1}",
                        "aaargh_last_one ${parent.param3} ${scatter.stuff} ${param1} > ${output2}",
                    ],
                    "outputs": {
                        "output1": "${parent.param2}_${param1}.out",
                        "output2": "${scatter.stuff}.${parent.param3}.log",
                    },
                    "skip_if_output_exists": True,
                    "compute": {
                        "cpus": 99,
                        "memory": 1024,
                        "spot": True,
                        "queue_name": "custom",
                    },
                },
            },
        ],
        "outputs": {
            "outputs": {
                "output1": "string_${job.whatever}_out.txt",
                "output2": "run_${param2}_${param3}.log",
                "output3": "42_${job.field}_${what_is_this}_string.out",
            },
        },
    }
    result = do_param_substitution(spec)
    expect = {
        "scatter": {
            "stuff": "file*.txt"
        },
        "params": {},
        "inputs": {
            "input1": "string.txt",
            "input2": "in_${job.field}.cfg",
            "input3": "42_things.lst",
        },
        "steps": [
            {
                "Step1": {
                    "image": "test-image",
                    "params": {
                        "param1": "step1_string",
                        "param2": "step1_${job.field}",
                    },
                    "inputs": {
                        "input1": "step1_input1_string.txt",
                        "input2": "step1_input2_${job.field}_42.lst",
                    },
                    "commands": [
                        "do_something ${scatter.stuff} string ${param1} ${parent.input1} > ${output1}",
                        "do_something_else ${parent.input2} ${job.field} ${scatter.stuff} ${param2} > ${output2}",
                    ],
                    "outputs": {
                        "output1": "step1_output1_${job.field}.out",
                        "output2": "step1_output2_42.log",
                    },
                    "skip_if_output_exists": True,
                    "compute": {
                        "cpus": 99,
                        "memory": 1024,
                        "spot": True,
                        "queue_name": "custom",
                    },
                },
                "Step2": {
                    "image": "test-image",
                    "params": {
                        "param1": "${job.field}_step2",
                        "param2": "42",
                    },
                    "inputs": {
                        "input1": "42_step2_input1.xyz",
                        "input2": "string.abc",
                    },
                    "commands": [
                        "do_more_stuff string ${param2} ${parent.input3} > ${output1}",
                        "aaargh_last_one 42 ${scatter.stuff} ${param1} > ${output2}",
                    ],
                    "outputs": {
                        "output1": "${job.field}_${param1}.out",
                        "output2": "${scatter.stuff}.42.log",
                    },
                    "skip_if_output_exists": True,
                    "compute": {
                        "cpus": 99,
                        "memory": 1024,
                        "spot": True,
                        "queue_name": "custom",
                    },
                },
            },
        ],
        "outputs": {
            "outputs": {
                "output1": "string_${job.whatever}_out.txt",
                "output2": "run_${job.field}_42.log",
                "output3": "42_${job.field}_${what_is_this}_string.out",
            },
        },
    }
    assert result == expect


@pytest.mark.parametrize("timestring, seconds", [
    ("70s", 70),
    ("20 m", 1200),
    ("3h", 3600*3),
    ("2 d", 86400*2),
    ("1w", 86400*7)
])
def test_time_string_to_seconds(timestring, seconds):
    result = time_string_to_seconds(timestring)
    assert result == seconds


@pytest.mark.parametrize("p_role, o_role, x_role", [
    (None, None, None),
    (None, "opt_role", "opt_role"),
    ("parm_role", None, "parm_role"),
    ("parm_role", "opt_role", "opt_role"),
])
def test_merge_params_and_options(p_role, o_role, x_role):
    params = {"a": 1, "b": 2, "c": 3, "task_role": p_role}
    options = {"z": 9, "y": 8, "task_role": o_role}
    expect = {"a": 1, "b": 2, "c": 3, "z": 9, "y": 8, "task_role": x_role}
    result = merge_params_and_options(params, options)
    assert result == expect

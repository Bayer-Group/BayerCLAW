import json
import pytest

from ...src.compiler.pkg.util import Step, make_logical_name, substitute_params, time_string_to_seconds, \
    merge_params_and_options


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
    (Step("name1", {"Other": "stuff", "inputs": {"file1": "one", "file2": "two"}}, ""), {"inputs": json.dumps({"file1": "one", "file2": "two"}, separators=(",", ":"))}),
    (Step("name2", {"Other": "stuff", "inputs": {}}, ""), {"inputs": json.dumps({})}),
    (Step("name3", {"Other": "stuff", "inputs": None}, ""), {"inputs.$": "States.JsonToString($.prev_outputs)"})
])
def test_step_input_field(step, expect):
    result = step.input_field
    assert result == expect


def test_make_logical_name():
    orig_name = "a-name  with++LOTS___of%wEiRd,,\n,,characters/that~will&NEVER(work)as\ta##LOGICAL!name12345"
    result = make_logical_name(orig_name)
    expect = "ANameWithLotsOfWeirdCharactersThatWillNeverWorkAsALogicalName12345"
    assert result == expect


def test_substitute_params():
    target = {
        "one": "${value1} ${value2} ${value3} ${skip_me} ${value1} again",
        "two": [
            "eh ${value1}",
            "bee ${value2}",
            "sea ${value3}",
            "dee ${skip_me}"
        ],
        "three": {
            "k1": "double-u ${value1}",
            "k2": "ecks ${value2}",
            "k3": "why ${value3}",
            "k4": "zee ${skip_me}"
        },
        "four": 99,
    }
    params = {
        "value1": "string",
        "value2": "${reference}",
        "value3": 42,
        "value4": "not used",
    }
    expect = {
        "one": "string ${reference} 42 ${skip_me} string again",
        "two": [
            "eh string",
            "bee ${reference}",
            "sea 42",
            "dee ${skip_me}"
        ],
        "three": {
            "k1": "double-u string",
            "k2": "ecks ${reference}",
            "k3": "why 42",
            "k4": "zee ${skip_me}"
        },
        "four": 99,
    }
    result = substitute_params(params, target)
    assert result == expect


def test_substitute_params_empty_params():
    params = {}
    target = "${one} ${two} ${three}"
    result = substitute_params(params, target)
    assert result == target


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

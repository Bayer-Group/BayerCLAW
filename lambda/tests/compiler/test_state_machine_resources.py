import pytest

from ...src.compiler.pkg.state_machine_resources import make_launcher_step, _stepperator, capitalize_key_inplace, \
    fill_in_nexts_and_ends
from ...src.compiler.pkg.util import CoreStack, Step, lambda_logging_block


def test_make_launcher_step(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    wf_params = {"repository": "s3://bucket/repo/path/${template}"}

    # todo: only return dict
    _, result = make_launcher_step(core_stack, wf_params, "need_next_step_now_but_probably_not_later")
    expect = {
        "Launch": {
            "Type": "Task",
            "Resource": "launcher_lambda_arn",
            "Parameters": {
                "repo_template": wf_params["repository"],
                "input_obj.$": "$",
                **lambda_logging_block("Launch"),

            },
            "ResultPath": "$",
            "OutputPath": "$",
            "_stet": True,
        },
    }

    assert result == expect


test_steps = [
    {"step1": {"test": "spec1"}},
    {"step2": {"test": "spec2"}},
    {"step3": {"test": "spec3"}},
]

@pytest.mark.parametrize("steps, expect", [
    (test_steps, ["step1", "step2", "step3"]),
    (reversed(test_steps), ["step3", "step2", "step1"]),
])
def test_stepperator(steps, expect):
    result = list(_stepperator(steps))

    assert all(isinstance(s, Step) for s in result)
    names = [s.name for s in result]
    assert names == expect

    in_specs = [next(iter(s.values())) for s in steps]
    out_specs = [s.spec for s in result]
    assert all(i is o for i, o in zip(in_specs, out_specs))


@pytest.mark.parametrize("key, expect", [
    ("test", {"Test": 1, "untest": 2}),
    ("whatevs", {"test": 1, "untest": 2}),
])
def test_capitalize_key_inplace(key, expect):
    data = {"test": 1, "untest": 2}
    capitalize_key_inplace(data, key)
    assert data == expect


def test_fill_in_nexts_and_ends():
    steps = [
        {"step1": {"data": "1"}},
        {"step2": {"data": "2"}},
        {"step3": {"data": "3", "next": "step5"}},
        {"step4": {"data": "4", "end": True}},
        {"step5": {"data": "5", "Next": "step7"}},
        {"step6": {"data": "6", "End": True}},
        {"step7": {"data": "7"}},
    ]
    expect = [
        {"step1": {"data": "1", "Next": "step2"}},
        {"step2": {"data": "2", "Next": "step3"}},
        {"step3": {"data": "3", "Next": "step5"}},
        {"step4": {"data": "4", "End": True}},
        {"step5": {"data": "5", "Next": "step7"}},
        {"step6": {"data": "6", "End": True}},
        {"step7": {"data": "7", "End": True}},
    ]
    fill_in_nexts_and_ends(steps)
    assert steps == expect

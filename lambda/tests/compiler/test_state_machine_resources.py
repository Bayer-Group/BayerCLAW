import pytest

from ...src.compiler.pkg.state_machine_resources import make_initializer_step, make_step_list
from ...src.compiler.pkg.util import CoreStack, Step, lambda_logging_block, lambda_retry


def test_make_initializer_step(monkeypatch, mock_core_stack):
    monkeypatch.setenv("CORE_STACK_NAME", "bclaw-core")
    core_stack = CoreStack()

    wf_params = {"repository": "s3://bucket/repo/path/${template}"}

    result = make_initializer_step(core_stack, wf_params)
    expect = {
        "Initialize": {
            "Type": "Task",
            "Resource": "initializer_lambda_arn",
            "Parameters": {
                "repo_template": wf_params["repository"],
                "input_obj.$": "$",
                **lambda_logging_block("Initialize"),
            },
            **lambda_retry(),
            "ResultPath": "$",
            "OutputPath": "$",
            "_stet": True,
        },
    }

    assert result == expect


def test_make_step_list():
    steps = [
        {"step1": {"data": "1"}},
        {"step2": {"data": "2"}},
        {"step3": {"data": "3", "next": "step5"}},
        {"step4": {"data": "4", "end": True}},
        {"step5": {"data": "5", "Next": "step7"}},
        {"step6": {"data": "6", "End": True}},
        {"step7": {"data": "7"}},
    ]
    expected_nexts = [
        "step2",
        "step3",
        "step5",
        "",
        "step7",
        "",
        "",
    ]

    results = make_step_list(steps)

    for orig, result, exp_next in zip(steps, results, expected_nexts):
        assert isinstance(result, Step)
        k, v = next(iter(orig.items()))
        assert result.name == k
        assert result.spec == v
        assert result.next == exp_next

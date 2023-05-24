import pytest

from ...src.compiler.pkg.state_machine_resources import make_initializer_step, make_step_list, \
    make_physical_name
from ...src.compiler.pkg.util import Step, lambda_logging_block, lambda_retry


def test_make_initializer_step(compiler_env):
    repository = "s3://bucket/repo/path/${template}"

    result = make_initializer_step(repository)
    expect = {
        "Initialize": {
            "Type": "Task",
            "Resource": "initializer_lambda_arn",
            "Parameters": {
                "repo_template": repository,
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


@pytest.mark.parametrize("versioned", ["true", "false"])
def test_make_physical_name(versioned):
    result = make_physical_name(versioned)
    if versioned == "true":
        expect = {
            "StateMachineName": {
                "Fn::Sub": [
                    "${Root}--${Version}",
                    {
                        "Root": {"Ref": "AWS::StackName"},
                        "Version": {
                            "Fn::GetAtt": ["launcherStack", "Outputs.LauncherLambdaVersion"],
                        },
                    },
                ],
            },
        }
    else:
        expect = {
            "StateMachineName": {"Ref": "AWS::StackName"}
        }
    assert result == expect
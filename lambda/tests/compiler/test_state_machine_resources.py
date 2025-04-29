import pytest

from ...src.compiler.pkg.state_machine_resources import (make_initializer_step, make_step_list,
                                                         state_machine_version_rc,
                                                         state_machine_alias_rc,
                                                         STATE_MACHINE_VERSION_NAME,
                                                         STATE_MACHINE_ALIAS_NAME)
from ...src.compiler.pkg.util import Step, Resource, lambda_logging_block, lambda_retry


def test_make_initializer_step(compiler_env):
    repository = "s3://bucket/repo/path/${template}"

    result = make_initializer_step(repository)
    expect = {
        "Initialize": {
            "Type": "Task",
            "Resource": "initializer_lambda_arn",
            "Parameters": {
                "workflow_name": "${WorkflowName}",
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


def test_state_machine_version_rc():
    state_machine = Resource("stateMachineLogicalName", {})
    result = state_machine_version_rc(state_machine)
    expect = Resource(STATE_MACHINE_VERSION_NAME,
                      {
                          "Type": "AWS::StepFunctions::StateMachineVersion",
                          "UpdateReplacePolicy": "Retain",
                          "Properties": {
                              "Description": "No description",
                              "StateMachineArn": {"Ref": "stateMachineLogicalName"},
                              "StateMachineRevisionId": {"Fn::GetAtt": ["stateMachineLogicalName", "StateMachineRevisionId"]},
                          },
                      })
    assert result == expect


def test_state_machine_alias_rc():
    state_machine_version = Resource(STATE_MACHINE_VERSION_NAME, {})
    result = state_machine_alias_rc(state_machine_version)
    expect = Resource(STATE_MACHINE_ALIAS_NAME,
                      {
                          "Type": "AWS::StepFunctions::StateMachineAlias",
                          "Properties": {
                              "Name": "current",
                              "Description": "Current active version",
                              "DeploymentPreference": {
                                  "StateMachineVersionArn": {"Ref": STATE_MACHINE_VERSION_NAME},
                                  "Type": "ALL_AT_ONCE",
                              },
                          },
                      })
    assert result == expect

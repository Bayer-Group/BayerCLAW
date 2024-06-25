import json

import boto3
import moto
import pytest

from ..src.runner.qc_check import abort_execution, run_one_qc_check, run_all_qc_checks, do_checks, QCFailure

QC_DATA_1 = {
    "a": 1,
    "b": 2.
}
QC_DATA_2 = {
    "x": 99,
    "y": 98,
}


@pytest.fixture(scope="function")
def mock_state_machine():
    with moto.mock_aws():
        iam = boto3.resource("iam", region_name="us-east-1")
        role = iam.create_role(
            RoleName="fakeRole",
            AssumeRolePolicyDocument="{}"
        )

        sfn = boto3.client("stepfunctions", region_name="us-east-1")
        state_machine = sfn.create_state_machine(
            name="fakeStateMachine",
            definition="{}",
            roleArn=role.arn
        )

        yield state_machine["stateMachineArn"]


@pytest.fixture(scope="function")
def mock_qc_data_files(mocker, request):
    qc_file1 = mocker.mock_open(read_data=json.dumps(QC_DATA_1))
    qc_file2 = mocker.mock_open(read_data=json.dumps(QC_DATA_2))
    ret = mocker.patch("builtins.open", qc_file1)
    ret.side_effect = [qc_file1.return_value, qc_file2.return_value]


def test_abort_execution(mock_state_machine, monkeypatch):
    sfn = boto3.client("stepfunctions", region_name="us-east-1")
    sfn_execution = sfn.start_execution(
        stateMachineArn=mock_state_machine,
        name="fake_execution",
        input='{"in": "put"}'
    )

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCOUNT_ID", "123456789012")
    monkeypatch.setenv("BC_WORKFLOW_NAME", "fakeStateMachine")
    monkeypatch.setenv("BC_EXECUTION_ID", "fake_execution")
    monkeypatch.setenv("BC_STEP_NAME", "test_step")

    abort_execution(["failure1", "failure2"])

    execution_desc = sfn.describe_execution(executionArn=sfn_execution["executionArn"])
    assert execution_desc["status"] == "ABORTED"


@pytest.mark.parametrize("expression, expect", [
    ("x == 1", True),
    ("x != 1", False),
])
def test_run_one_qc_check(expression, expect):
    qc_data = {"x": 1}
    result = run_one_qc_check(qc_data, expression)
    assert result == expect


@pytest.mark.parametrize("fake1_cond, fake2_cond, expect", [
    (["a>1"], ["x<99"], []),  # all pass
    (["a>1", "b==2"], ["y<98"], ["fake1: b==2"]),  # one fail
    (["b==1"], ["x==99", "y==98"], ["fake2: x==99", "fake2: y==98"]),  # multi fail
    (["a==1", "b==2"], ["x==99", "y==98"], ["fake1: a==1", "fake1: b==2", "fake2: x==99", "fake2: y==98"]),  # all fail
])
def test_run_all_qc_checks(fake1_cond, fake2_cond, expect, mock_qc_data_files):
    spec = [
        {
            "qc_result_file": "fake1",
            "stop_early_if": fake1_cond,
        },
        {
            "qc_result_file": "fake2",
            "stop_early_if": fake2_cond,
        },
    ]

    result = list(run_all_qc_checks(spec))
    assert result == expect


@pytest.mark.parametrize("fake1_cond, fake2_cond, expect", [
    (None, None, []),  # no checks
    (["a>1"], ["x<99"], []),  # all pass
    (["a>1", "b==2"], ["y<98"], ["fake1: b==2"]),  # one fail
    (["b==1"], ["x==99", "y==98"], ["fake2: x==99", "fake2: y==98"]),  # multi fail
    (["a==1", "b==2"], ["x==99", "y==98"],
     ["fake1: a==1", "fake1: b==2", "fake2: x==99", "fake2: y==98"]),  # all fail
])
def test_do_checks(fake1_cond, fake2_cond, expect, mock_qc_data_files, mocker):
    mock_abort_execution = mocker.patch("bclaw_runner.src.runner.qc_check.abort_execution")

    if fake1_cond is None:
        spec = []
    else:
        spec = [
            {
                "qc_result_file": "fake1",
                "stop_early_if": fake1_cond,
            },
            {
                "qc_result_file": "fake2",
                "stop_early_if": fake2_cond,
            },
        ]

    if expect:
        with pytest.raises(QCFailure) as qcf:
            do_checks(spec)
        assert qcf.value.failures == expect
    else:
        do_checks(spec)

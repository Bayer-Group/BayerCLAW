import json

import boto3
import moto
import pytest

from ..src.runner.qc_check import abort_execution, run_one_qc_check, run_all_qc_checks, do_checks

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


# def test_run_qc_checks(mock_qc_data_files, mocker):
#     mock_run_qc_check = mocker.patch("bclaw_runner.src.runner.qc_check.run_qc_check")
#
#     spec = [
#         {
#             "qc_result_file": "fake1",
#             "stop_early_if": [
#                 "b == 1",
#             ],
#         },
#         {
#             "qc_result_file": "fake2",
#             "stop_early_if": [
#                 "x == 99",
#                 "y == 98",
#             ],
#         },
#     ]
#     run_qc_checks(spec)
#     result = mock_run_qc_check.call_args_list
#     expect = [
#         mocker.call(QC_DATA_1, "b == 1"),
#         mocker.call(QC_DATA_2, "x == 99"),
#         mocker.call(QC_DATA_2, "y == 98"),
#     ]
#     assert result == expect


# def test_run_qc_checks_empty(mock_qc_data_files, mocker):
#     mock_run_qc_check = mocker.patch("bclaw_runner.src.runner.qc_check.run_qc_check")
#     run_qc_checks([])
#     mock_run_qc_check.assert_not_called()


# @pytest.mark.parametrize("expression, expect_abort", [
#     ("x == 1", False),
#     ("x > 1", True),
# ])
# def test_run_qc_check(expression, expect_abort, mocker):
#     mock_abort_execution = mocker.patch("bclaw_runner.src.runner.qc_check.abort_execution")
#
#     qc_data = {"x": 1}
#     run_qc_check(qc_data, expression)
#
#     if expect_abort:
#         mock_abort_execution.assert_not_called()
#     else:
#         mock_abort_execution.assert_called()


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
    (None, None, False),  # no checks
    (["a>1"], ["x<99"], False),  # all pass
    (["a>1", "b==2"], ["y<98"], True),  # one fail
    (["b==1"], ["x==99", "y==98"], True),  # multi fail
    (["a==1", "b==2"], ["x==99", "y==98"], True),  # all fail
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

    do_checks(spec)
    if expect:
        mock_abort_execution.assert_called_once()
    else:
        mock_abort_execution.assert_not_called()

import time

import jmespath
import pytest
import requests

from ..src.runner import instance


def test_get_imdsv2_token(requests_mock, monkeypatch):
    monkeypatch.setattr(instance, "TOKEN", None)
    requests_mock.put("http://169.254.169.254/latest/api/token", text="mocked-token")

    instance.get_imdsv2_token()
    assert instance.TOKEN == "mocked-token"


def test_get_imdsv2_token_failure(requests_mock, monkeypatch):
    monkeypatch.setattr(instance, "TOKEN", None)
    requests_mock.put("http://169.254.169.254/latest/api/token", status_code=500)
    instance.get_imdsv2_token()
    assert instance.TOKEN is None


@pytest.mark.parametrize("response_text, expected", [
    ("spot", True),
    ("on-demand", False),
])
def test_this_is_a_spot_instance(requests_mock, response_text, expected):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text=response_text)
    assert instance._this_is_a_spot_instance() == expected


def test_this_is_a_spot_instance_failure(requests_mock):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", status_code=500)
    assert instance._this_is_a_spot_instance() is False


def test_do_termination_check_no_warning(requests_mock, caplog):
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=404)
    with caplog.at_level("DEBUG"):
        instance._do_termination_check()
        assert "not terminated" in caplog.text


def test_do_termination_check_with_warning(requests_mock, caplog):
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action",
                      status_code=200, json={"time": "2024-01-01T00:00:00Z"})
    with caplog.at_level("WARNING"):
        instance._do_termination_check()
        assert "spot instance will be terminated at 2024-01-01T00:00:00Z" in caplog.text


def test_do_termination_check_token_refresh(mocker, requests_mock):
    token_getter = mocker.patch("bclaw_runner.src.runner.instance.get_imdsv2_token", return_value=None)
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=401)
    instance._do_termination_check()
    token_getter.assert_called_once()


def test_do_termination_check_other_failure(requests_mock, caplog):
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action",
                        status_code=500, reason="Internal Server blah blah")
    with pytest.raises(requests.exceptions.RequestException):
         instance._do_termination_check()


def test_termination_checker_impl(mocker):
    do_check = mocker.patch("bclaw_runner.src.runner.instance._do_termination_check", return_value=None)
    event = mocker.MagicMock()
    event.is_set.side_effect = [False, False, True]  # Run loop twice then exit
    instance._termination_checker_impl(event, interval=0)
    assert do_check.call_count == 2


def test_termination_checker_impl_with_exception(mocker, caplog):
    do_check = mocker.patch("bclaw_runner.src.runner.instance._do_termination_check",
                            side_effect=Exception("Test Exception"))
    event = mocker.MagicMock()
    event.is_set.side_effect = [False, False, True]  # Run loop twice then exit
    with caplog.at_level("WARNING"):
        instance._termination_checker_impl(event, interval=0)
        assert "termination check failed" in caplog.text
    assert do_check.call_count == 2  # Ensure it was called twice despite exceptions


def test_spot_termination_checker(monkeypatch, requests_mock, caplog):
    monkeypatch.setattr(instance, "TOKEN", "test_token")
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text="spot")
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", [
        {"json": {}, "status_code": 404},
        {"reason": "internal server blah blah", "status_code": 500},
        {"json": {"time": "12:34:56"}, "status_code": 200},
    ])

    # the status_code = 500 iteration produces the error message that codebuild (formerly) highlights

    with caplog.at_level("DEBUG"):
        with instance.spot_termination_checker(interval=1):
            time.sleep(4)

        imsgs = enumerate(caplog.messages)

        # Find the index of the first message containing the specified string in the captured log messages.
        # Return None if no matching message is found
        i1 = next((i for i, v in imsgs if "not terminated" in v), None)
        i2 = next((i for i, v in imsgs if "termination check failed" in v), None)
        i3 = next((i for i, v in imsgs if "spot instance will be terminated at" in v), None)

        assert i1 is not None, "'not terminated' message not found"
        assert i2 is not None, "'termination check failed' message not found"
        assert i3 is not None, "'instance will be terminated' message not found"
        assert i1 < i2 < i3


def test_spot_termination_checker_no_token(monkeypatch, mocker, caplog):
    monkeypatch.setattr(instance, "TOKEN", None)
    termination_checker = mocker.patch("bclaw_runner.src.runner.instance._termination_checker_impl", return_value=None)
    # requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", status_code=401)
    # requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=401)

    with caplog.at_level("DEBUG"):
        with instance.spot_termination_checker():
            time.sleep(1)
    assert("continuing without spot termination checker" in caplog.text)
    termination_checker.assert_not_called()


def test_spot_termination_checker_stops_on_main_thread_crash(monkeypatch, requests_mock):
    monkeypatch.setattr(instance, "TOKEN", "test_token")
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text="spot")
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=404)

    with pytest.raises(RuntimeError, match="main thread failed"):
        with instance.spot_termination_checker():
            time.sleep(1)
            raise RuntimeError("main thread failed")


def crashy_termination_checker_impl(*_):
    time.sleep(1)
    raise RuntimeError("thread crash")


def test_spot_termination_checker_thread_crash(requests_mock, monkeypatch):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text="spot")
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=404)
    monkeypatch.setattr("bclaw_runner.src.runner.instance._termination_checker_impl", crashy_termination_checker_impl)

    success = False
    with instance.spot_termination_checker():
        time.sleep(2)
        success = True

    assert success


@pytest.mark.parametrize("wf_name, step_name, expect", [
    ("testWf", "testStep", "testWf.testStep"),
    ("testWf", None, "testWf.undefined"),
    (None, "testStep", "undefined.testStep"),
    (None, None, "undefined.undefined"),
])
def test_tag_this_instance(monkeypatch, mock_ec2_instance, requests_mock, wf_name, step_name, expect):
    monkeypatch.setattr(instance, "TOKEN", "test_token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    if wf_name is not None:
        monkeypatch.setenv("BC_WORKFLOW_NAME", wf_name)
    if step_name is not None:
        monkeypatch.setenv("BC_STEP_NAME", step_name)

    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-id", text=mock_ec2_instance.id)
    instance.tag_this_instance()

    mock_ec2_instance.load()
    name_tag = jmespath.search("[?Key=='Name'].Value", mock_ec2_instance.tags)[0]
    assert name_tag == expect


def test_tag_this_instance_no_token(monkeypatch, caplog):
    monkeypatch.setattr(instance, "TOKEN", None)
    with caplog.at_level("WARNING"):
        instance.tag_this_instance()
    assert "No token available, skipping instance tagging" in caplog.text


@pytest.mark.parametrize("problem", [
    {"status_code": 500},
    {"exc": requests.exceptions.ConnectTimeout},
    {"text": "not-a-valid-instance-id"}
])
def test_tag_this_instance_never_crashes(monkeypatch, requests_mock, problem, caplog):
    monkeypatch.setattr(instance, "TOKEN", "test_token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-id", **problem)
    instance.tag_this_instance()
    assert "unable to tag instance, continuing" in caplog.text

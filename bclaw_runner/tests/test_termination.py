import logging
import time

import pytest
import requests


from ..src.runner.termination import _this_is_a_spot_instance, _do_termination_check, spot_termination_checker


@pytest.mark.parametrize("text, expect", [
    ("spot", True),
    ("on-demand", False),
])
def test_this_is_a_spot_instance_ec2(requests_mock, text, expect):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text=text)
    response = _this_is_a_spot_instance()
    assert response == expect


def test_this_is_a_spot_instance_not_ec2(requests_mock):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle",
                      exc=requests.exceptions.ConnectTimeout)
    response = _this_is_a_spot_instance()
    assert not response


@pytest.mark.parametrize("json, status_code, expect", [
    ({"something": "something", "time": "12:34:56.7890"}, 200, "spot instance will be terminated at"),
    ({}, 404, "not terminated"),
])
def test_do_termination_check(requests_mock, caplog, json, status_code, expect):
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action",
                      status_code=status_code, json=json)

    with caplog.at_level(logging.DEBUG):
        _do_termination_check()
        assert caplog.messages[-1].startswith(expect)


def test_spot_termination_checker(requests_mock, caplog):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text="spot")
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", [
        {"json": {}, "status_code": 404},
        {"reason": "internal server blah blah", "status_code": 500},
        {"json": {"time": "12:34:56"}, "status_code": 200},
    ])

    # the status_code = 500 iteration produces the error message that codebuild (formerly) highlights

    with caplog.at_level(logging.DEBUG):
        with spot_termination_checker(interval=1):
            time.sleep(4)

        imsgs = enumerate(caplog.messages)

        # Find the index of the first message containing the specified string in the captured log messages.
        # Return None if no matching message is found
        i1 = next((i for i, v in imsgs if "not terminated" in v), None)
        i2 = next((i for i, v in imsgs if "termination warning check failed" in v), None)
        i3 = next((i for i, v in imsgs if "spot instance will be terminated at" in v), None)

        assert i1 is not None, "'not terminated' message not found"
        assert i2 is not None, "'warning check failed' message not found"
        assert i3 is not None, "'instance will be terminated' message not found"
        assert i1 < i2 < i3


def test_spot_termination_checker_main_crash(requests_mock):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text="spot")
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=404)

    with pytest.raises(RuntimeError, match="failed"):
        with spot_termination_checker():
            time.sleep(1)
            raise RuntimeError("failed")


def crashy_termination_checker_impl(*_):
    time.sleep(1)
    raise RuntimeError("thread crash")


def test_spot_termination_checker_thread_crash(requests_mock, monkeypatch):
    requests_mock.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", text="spot")
    requests_mock.get("http://169.254.169.254/latest/meta-data/spot/instance-action", status_code=404)
    monkeypatch.setattr("bclaw_runner.src.runner.termination._termination_checker_impl", crashy_termination_checker_impl)

    success = False
    with spot_termination_checker():
        time.sleep(2)
        success = True

    assert success

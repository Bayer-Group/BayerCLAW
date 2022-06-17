import boto3
import jmespath
import moto
import pytest
import requests

from ..src.runner.tagging import INSTANCE_ID_URL, tag_this_instance


@pytest.fixture(scope="module")
def mock_ec2_instance():
    with moto.mock_ec2():
        ec2 = boto3.resource("ec2")
        instances = ec2.create_instances(ImageId="ami-12345", MinCount=1, MaxCount=1)
        yield instances[0]


@pytest.mark.parametrize("wf_name, step_name, expect", [
    ("testWf", "testStep", "testWf.testStep"),
    ("testWf", None, "testWf.undefined"),
    (None, "testStep", "undefined.testStep"),
    (None, None, "undefined.undefined"),
])
def test_tag_this_instance(monkeypatch, mock_ec2_instance, requests_mock, wf_name, step_name, expect):
    if wf_name is not None:
        monkeypatch.setenv("BC_WORKFLOW_NAME", wf_name)
    if step_name is not None:
        monkeypatch.setenv("BC_STEP_NAME", step_name)

    requests_mock.get(INSTANCE_ID_URL, text=mock_ec2_instance.id)
    tag_this_instance()

    mock_ec2_instance.load()
    name_tag = jmespath.search("[?Key=='Name'].Value", mock_ec2_instance.tags)[0]
    assert name_tag == expect


@pytest.mark.parametrize("problem", [
    {"status_code": 500},
    {"exc": requests.exceptions.ConnectTimeout},
    {"text": "not-a-valid-instance-id"}
])
def test_tag_this_instance_never_crashes(requests_mock, problem, caplog):
    requests_mock.get(INSTANCE_ID_URL, **problem)
    tag_this_instance()
    assert "unable to tag image, continuing" in caplog.text

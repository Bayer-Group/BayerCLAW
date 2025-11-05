from contextlib import contextmanager
import logging
import os
import threading
from typing import Generator

import backoff
import boto3
import requests

logger = logging.getLogger(__name__)

METADATA_HOME = "169.254.169.254"
TOKEN = None


@backoff.on_exception(backoff.constant, requests.exceptions.RequestException,
                      max_tries=3, interval=1, raise_on_giveup=False)
def get_imdsv2_token(ttl_seconds: int = 21600) -> None:
    """Fetches and sets the IMDSv2 token."""
    global TOKEN
    logger.debug("Getting IMDSv2 token")

    response = requests.put(
        f"http://{METADATA_HOME}/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": str(ttl_seconds)},
        timeout=1
    )
    response.raise_for_status()
    TOKEN = response.text


def _this_is_a_spot_instance() -> bool:
    """Checks if the instance is a spot instance."""

    # note: AEMM does not properly support the instance-life-cycle endpoint,
    # so stub this out when running in that environment
    if METADATA_HOME == "localhost:1338":
        return False

    try:
        response = requests.get(
            f"http://{METADATA_HOME}/latest/meta-data/instance-life-cycle",
            headers={"X-aws-ec2-metadata-token": TOKEN},
            timeout=1,
        )
        response.raise_for_status()
        return response.text == "spot"
    except requests.exceptions.RequestException:
        logger.warning("unable to determine if this is a spot instance; assuming it's not")
        return False


def _do_termination_check() -> None:
    """Checks if the spot instance is scheduled for termination."""
    response = requests.get(
        f"http://{METADATA_HOME}/latest/meta-data/spot/instance-action",
        headers={"X-aws-ec2-metadata-token": TOKEN},
        timeout=1,
    )
    if response.status_code == 200:
        logger.warning(f"spot instance will be terminated at {response.json()['time']}")
    elif response.status_code == 401:
        # token might be invalid or expired, go get a new one
        # don't bother retrying the request, we'll just check again on the next interval
        get_imdsv2_token()
    elif response.status_code == 404:
        # expect a 404 when termination is not scheduled
        logger.debug("not terminated")
    else:
        # raise for other errors
        response.raise_for_status()


def _termination_checker_impl(event, interval) -> None:
    """Runs the termination check in a loop."""
    while not event.is_set():
        try:
            logger.debug("checking instance metadata")
            _do_termination_check()
        except Exception as e:
            logger.warning(f"termination check failed: {str(e)}")
        finally:
            event.wait(interval)
    logger.debug("exiting checker thread")


@contextmanager
def spot_termination_checker(interval=30) -> Generator[None, None, None]:
    """Context manager for the spot termination checker."""
    if TOKEN is None:
        logger.warning("No token available; continuing without spot termination checker")
        yield
    elif _this_is_a_spot_instance():
        stopper = threading.Event()
        thread = threading.Thread(target=_termination_checker_impl, args=(stopper, interval))
        logger.debug("starting checker thread")
        thread.start()
        try:
            yield
        finally:
            logger.debug("stopping checker thread")
            stopper.set()
            thread.join()
            logger.debug("checker thread stopped")
    else:
        logger.debug("This is not a spot instance: checker thread not started")
        yield


def tag_this_instance():
    """Tags the current instance with workflow and step names."""
    if TOKEN is None:
        logger.warning("No token available, skipping instance tagging")
        return

    try:
        response = requests.get(
            f"http://{METADATA_HOME}/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": TOKEN},
            timeout=1,
        )
        response.raise_for_status()
        instance_id = response.text
        logger.info(f"{instance_id=}")

        instance_tag = ".".join([os.environ.get("BC_WORKFLOW_NAME", "undefined"),
                                 os.environ.get("BC_STEP_NAME", "undefined")])

        ec2 = boto3.resource("ec2")
        instance = ec2.Instance(instance_id)
        instance.create_tags(Tags=[{"Key": "Name", "Value": instance_tag}])

    except Exception:
        logger.warning("unable to tag instance, continuing...")


if __name__ == "__main__":
    # this is for testing with an amazon ec2 metadata mock (AEMM) container:
    #   https://github.com/aws/amazon-ec2-metadata-mock
    #   docker run -it --rm -p 1338:1338 public.ecr.aws/aws-ec2/amazon-ec2-metadata-mock:v1.13.0 spot -I -d 15
    import time

    logging.basicConfig(level=logging.DEBUG)

    METADATA_HOME = "localhost:1338"
    TOKEN_TIMEOUT = 15
    # TOKEN_TIMEOUT = 999999  # this will crash the token request

    get_imdsv2_token(TOKEN_TIMEOUT)
    logger.info(f"IMDSv2 token: {TOKEN}")
    tag_this_instance()

    with spot_termination_checker(interval=3):
        for t in range(60):
            logger.info(f"Working ({t})...")
            time.sleep(1)
    logger.info("finished")

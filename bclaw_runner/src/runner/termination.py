from contextlib import contextmanager
import logging
import threading

import requests

logger = logging.getLogger(__name__)

METADATA_HOME = "http://169.254.169.254"


def _this_is_a_spot_instance() -> bool:
    # note: AEMM does not support the instance-life-cycle endpoint
    if METADATA_HOME == "http://localhost:1338":
        return True

    try:
        response = requests.get("http://169.254.169.254/latest/meta-data/instance-life-cycle", timeout=1)
        return response.text == "spot"
    except requests.exceptions.ConnectTimeout:
        return False


def _do_termination_check() -> None:
    response = requests.get(f"{METADATA_HOME}/latest/meta-data/spot/instance-action")
    if response.status_code == 200:
        logger.warning(f"spot instance will be terminated at {response.json()['time']}")
    elif response.status_code == 404:
        # expect a 404 when termination is not scheduled
        logger.debug("not terminated")
    else:
        response.raise_for_status()


def _termination_checker_impl(event: threading.Event, interval: int) -> None:
    stopped = event.is_set()
    while not stopped:
        try:
            logger.debug("checking instance metadata")
            _do_termination_check()

        except Exception:
            logger.warning(f"termination warning check failed")

        finally:
            stopped = event.wait(interval)
            logger.debug("looping")
    logger.debug("exiting checker thread")


@contextmanager
def spot_termination_checker(interval: int = 30) -> None:
    if _this_is_a_spot_instance():
        stopper = threading.Event()
        checker_thread = threading.Thread(target=_termination_checker_impl, args=(stopper, interval))
        try:
            logger.debug("starting checker thread")
            checker_thread.start()
            yield

        except Exception:
            raise

        finally:
            logger.debug("stopping checker thread")
            stopper.set()
            checker_thread.join()
            logger.debug("checker thread stopped")
    else:
        logger.debug("this is not a spot instance: checker thread not started")
        yield


if __name__ == "__main__":
    # this is for testing with an amazon ec2 metadata mock (AEMM) container:
    #   https://github.com/aws/amazon-ec2-metadata-mock
    #   docker run -it --rm -p 1338:1338 amazon/amazon-ec2-metadata-mock:v1.2.0 spot -d 15
    import time

    METADATA_HOME = "http://localhost:1338"

    logging.basicConfig(level=logging.DEBUG)
    with spot_termination_checker(interval=3):
        for _ in range(20):
            print("working...")
            time.sleep(1)
        logger.debug("finished")
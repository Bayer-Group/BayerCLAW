import logging
import os

import boto3
import requests

logger = logging.getLogger(__name__)

INSTANCE_ID_URL = "http://169.254.169.254/latest/meta-data/instance-id"


def tag_this_instance():
    try:
        response = requests.get(INSTANCE_ID_URL, timeout=1)
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

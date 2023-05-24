import logging

from lambda_logs import JSONFormatter, custom_lambda_logs
from pkg.launcher_stuff import main

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers[0].setFormatter(JSONFormatter())


def lambda_handler(event: dict, context: object) -> None:
    with custom_lambda_logs(**event):
        logger.info(f"{event=}")
        main(event, context)

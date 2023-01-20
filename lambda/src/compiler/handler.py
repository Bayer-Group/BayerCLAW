import logging

from pkg.compiler import compile_template

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: dict, context: object) -> dict:
    # event = {
    #   accountId: str
    #   fragment: {
    #       Repository: str
    #       Parameters: {...}
    #       Options: {...}
    #       Steps: []
    #   }
    #   region: str
    #   params: {}  # empty
    #   requestId: uuid,
    #   templateParameterValues: {
    #       param1: value1
    #       param2: value2
    #       ...
    #   }
    #   transformId: str
    # }

    ret = event.copy()

    try:
        ret["fragment"] = compile_template(event["fragment"])
        ret["status"] = "success"

    except Exception as e:
        # https://stackoverflow.com/questions/55190232/aws-cloudformation-transform-how-do-i-properly-return-an-error-message
        logger.exception("failed: ")
        ret["status"] = "failure"
        ret["errorMessage"] = str(e)

    finally:
        return ret

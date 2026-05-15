import logging

USER_CMD_LEVEL = logging.INFO + 5
logging.addLevelName(USER_CMD_LEVEL, "USER_CMD")


class UserCmdHandler(logging.StreamHandler):
    def __init__(self):
        super().__init__()
        formatter = logging.Formatter("%(levelname)s: %(message)s")
        self.setFormatter(formatter)
        self.setLevel(logging.DEBUG)


# https://medium.com/@aman.deep291098/python-custom-logging-made-easy-c89f4972af95
class UserCmdLogger(logging.Logger):
    def __init__(self):
        super().__init__("user_cmd")
        self.setLevel(logging.DEBUG)
        self.propagate = False
        handler = UserCmdHandler()
        self.addHandler(handler)

    def user_cmd(self, msg, *args, **kwargs):
        if self.isEnabledFor(USER_CMD_LEVEL):
            self._log(USER_CMD_LEVEL, msg, args, **kwargs)

import json
import os

import pytest


@pytest.fixture(scope="session")
def read_config() -> dict:
    shell_name = os.environ.get("SHELL_NAME", "bash")
    cfg_path = os.path.realpath(
        os.path.join(
            os.path.dirname(__file__),  # HOME/bclaw_runner/tests/
            os.path.pardir,             # HOME/bclaw_runner/
            "src",                      # HOME/bclaw_runner/src
            "runner",                   # HOME/bclaw_runner/src/runner/
            "cfg",                      # HOME/bclaw_runner/src/runner/cfg/
            shell_name,                 # HOME/bclaw_runner/src/runner/cfg/<shell>/
            "cfg.json"                  # HOME/bclaw_runner/src/runner/cfg/<shell>/cfg.json
        )
    )
    with open(cfg_path) as fp:
        ret = json.load(fp)
    return ret

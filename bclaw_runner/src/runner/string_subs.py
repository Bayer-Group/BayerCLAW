import jmespath
import re
from typing import Any

SUB_FINDER = re.compile(r"\${(.+?)}")


def substitute(target: Any, spec: dict) -> Any:
    if isinstance(target, str):
        ret = SUB_FINDER.sub(lambda m: str(jmespath.search(m.group(1), spec) or m.group(0)), target)
    elif isinstance(target, list):
        ret = [substitute(v, spec) for v in target]
    elif isinstance(target, dict):
        ret = {k: substitute(v, spec) for k, v in target.items()}
    else:
        ret = target

    return ret

from functools import partial
import jmespath
import re
from typing import Any

SUB_FINDER = re.compile(r"\${(.+?)}")

# todo: use glom instead?
#       https://pypi.org/project/glom/
def lookup(m: re.Match, spec: dict) -> str:
    ret = jmespath.search(m.group(1), spec)
    if ret is None:
        ret = m.group(0)
    return str(ret)


def substitute(target: Any, spec: dict) -> Any:
    if isinstance(target, str):
        _lookup = partial(lookup, spec=spec)
        ret = SUB_FINDER.sub(_lookup, target)
    elif isinstance(target, list):
        ret = [substitute(v, spec) for v in target]
    elif isinstance(target, dict):
        ret = {k: substitute(v, spec) for k, v in target.items()}
    else:
        ret = target

    return ret

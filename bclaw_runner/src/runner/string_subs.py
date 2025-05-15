from functools import partial
import jmespath
import re
from typing import Any


def lookup(m: re.Match, spec: dict) -> str:
    ret = jmespath.search(m.group(1), spec)
    if ret is None:
        ret = m.group(0)
    return str(ret)


SUB_FINDER = re.compile(r"\${(.+?)}")

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


def substitute_image_tag(image_spec: dict, sub_spec: dict) -> dict:
    image_tag = image_spec["tag"]
    parts = image_tag.split("/")
    name_ver = parts.pop(-1)
    _lookup = partial(lookup, spec=sub_spec)
    subbed = SUB_FINDER.sub(_lookup, name_ver)

    ret = image_spec.copy()
    ret["tag"] = "/".join(parts + [subbed])
    return ret

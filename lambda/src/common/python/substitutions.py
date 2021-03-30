from functools import partial
import jmespath
import json
import re
from string import Template
from typing import Any


def _lookup(target: str, data: dict):
    ret0 = jmespath.search(target, data)

    if ret0 is None:
        raise RuntimeError(f"{target} not found in job data")
    elif isinstance(ret0, (dict, list)):
        ret = json.dumps(json.dumps(ret0))
    else:
        ret = str(ret0)

    return ret


JOB_DATA_FINDER = re.compile(r"\${(.+?)}")

def substitute_job_data(subject: Any, job_data: dict):
    lookup = partial(_lookup, data=job_data)

    if isinstance(subject, str):
        result = JOB_DATA_FINDER.sub(lambda m: lookup(m.group(1)), subject)

    elif isinstance(subject, list):
        result = [substitute_job_data(v, job_data) for v in subject]

    elif isinstance(subject, dict):
        result = {k: substitute_job_data(v, job_data) for k, v in subject.items()}

    else:
        result = subject

    return result


def substitute_into_filenames(subject: Any, subs: dict):
    if isinstance(subject, str):
        try:
            result = Template(subject).safe_substitute(subs)

        except KeyError:
            raise RuntimeError(f"unrecognized substitution in {subject}")

    elif isinstance(subject, list):
        result = [substitute_into_filenames(v, subs) for v in subject]

    elif isinstance(subject, dict):
        result = {k: substitute_into_filenames(v, subs) for k, v in subject.items()}

    else:
        result = subject

    return result

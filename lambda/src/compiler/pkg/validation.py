from collections import Counter
from functools import reduce
from itertools import chain
from typing import Any, Callable

from voluptuous import *

from .util import Step

DEFAULT_IMAGE = "public.ecr.aws/ubuntu/ubuntu:latest"


class CompilerError(Exception):
    def __init__(self, invalid_exception: Invalid, where=None):
        super().__init__(invalid_exception.msg)
        self.where = where
        self.field = ".".join(str(p) for p in invalid_exception.path)
        self.message = invalid_exception.error_message

    def __str__(self) -> str:
        if self.where is not None:
            if self.field:
                ret = f"in {self.where}, field '{self.field}': {self.message}"
            else:
                ret = f"in {self.where}: {self.message}"
        else:
            if self.field:
                ret = f"field '{self.field}': {self.message}"
            else:
                ret = self.message
        return ret


def listified(validator, min: int = 0) -> Callable:
    listy_validator = Schema([validator])
    def f(v: Any) -> list:
        if not isinstance(v, list):
            v = [v]
        if len(v) < min:
            raise ValueError("not enough items in list")
        return listy_validator(v)
    return f


def no_shared_keys(*field_names: str) -> Callable:
    def _impl(record: dict) -> dict:
        key_iters = ((record.get(f) or {}).keys() for f in field_names)
        keys = chain(*key_iters)
        key_counts = Counter(keys)
        dupes = [k for k, ct in key_counts.items() if ct > 1]
        if dupes:
            raise Invalid(f"duplicated keys: {', '.join(sorted(dupes))}")
        return record
    return _impl


def no_substitutions(s: str) -> str:
    if re.search(r"\${.+}", s):
        raise Invalid("string substitutions are not allowed")
    return s


image_credentials = re.compile(r"^(?!\+)(?P<name>\S+?)(?:\s+\+auth:\s+(?P<auth>[A-Za-z0-9/_+=.@-]+))?$")

def shorthand_image_spec(spec: str) -> dict:
    spec = spec.strip()
    if m := image_credentials.fullmatch(spec):
        name = m.group("name") or DEFAULT_IMAGE
        auth = m.group("auth") or ""
        return {"name": name, "auth": auth}
    raise Invalid(f"invalid image spec: '{spec}'")


splitter = re.compile(r"(?<=\s)\+(\w+):\s+")
src_dest = re.compile(r"^(\S+?(?<![>/])) (?:\s+->\s+ (s3://.+/))?$", flags=re.X)

def shorthand_output_spec(spec: str) -> dict:
    ret = {}
    file, *tags = splitter.split(spec)

    if m := src_dest.fullmatch(file.strip()):
        src, dest = m.groups()
        ret["name"] = src.strip()
        if dest is not None:
            ret["dest"] = dest.strip()
    else:
        raise Invalid(f"invalid filename spec: '{file}'")

    ret["s3_tags"] = {}
    for k, v in itertools.batched(tags, 2):
        ret["s3_tags"][k.strip()] = v.strip()

    return ret


def s3_path(v: str) -> str:
    if not isinstance(v, str) or not v.startswith("s3://"):
        raise Invalid("must be an S3 path starting with 's3://'")
    return v


output_spec = Schema({
    str: Or(
        And(str, shorthand_output_spec),
        {
            Required("name", msg="output file name is required"): str,
            Optional("dest"): s3_path,
            Optional("s3_tags", default={}): {str: str},
        }
    )
})


def file_list(validator) -> Callable:
    dicty_validator = Schema(validator)
    def _f(v: dict | list) -> dict:
        try:
            if isinstance(v, list):
                # merge list of dicts into single dict. The 'or r' trick returns
                # the updated dict (because dict.update() returns None).
                v = reduce(lambda r, dic: r.update(dic) or r, v, {})
        except Exception as e:
            raise Invalid("expected list of dicts") from e
        ret = dicty_validator(v)
        return ret
    return _f


skip_msg = "only one of 'skip_on_rerun' or 'skip_if_output_exists' is allowed"
next_or_end_msg = "cannot specify both 'next' and 'end' in a step"

next_or_end = {
    Exclusive("next", "next_or_end", msg=next_or_end_msg): str,
    Exclusive("end", "next_or_end", msg=next_or_end_msg): All(Boolean(), Msg(True, "'end' value must be truthy")),
}

qc_check_block = {
    Required("qc_result_file"): str,
    Required("stop_early_if"): listified(str, min=1)
}

filesystem_block = {
    Required("efs_id", msg="EFS filesystem ID is required"): All(str, Match(r"^fs-[0-9a-fA-F]+$")),
    Required("host_path", msg="host path for EFS mount is required"): All(str,
                                                                          Match(r"^/", msg="host_path must be fully qualified"),
                                                                          no_substitutions),
    Optional("root_dir", default="/"): All(str,
                                           Match(r"^/", msg="root_dir mut be a fully qualified path"),
                                           no_substitutions),
}

batch_step_schema = Schema(All(
    {
        Optional("image", default={"name": DEFAULT_IMAGE}): Or(
            And(str, shorthand_image_spec),
            {
                Required("name", msg="image name not found"): str,
                Optional("auth", default=""): str,
            }
        ),
        Optional("task_role", default=None): Maybe(str),
        # None is used as a signal that inputs was not specified at all, and should be copied from previous outputs.
        # inputs = {} can be used to explicitly specify a step has no inputs at all, with no copy from previous output.
        ## remove  Optional("inputs", default=None): Any(None, {str: str}),
        Optional("inputs", default=None): file_list(Any(None, {str: str})),
        ## remove  Optional("references", default={}): {str: Match(r"^s3://", msg="reference values must be s3 paths")},
        Optional("references", default={}): file_list({str: s3_path}),
        Required("commands", msg="commands list is required"): listified(str, min=1),
        Optional("s3_tags", default={}): {str: Coerce(str)},
        Optional("job_tags", default={}): {str: Coerce(str)},
        ## remove  Optional("outputs", default={}): {
        #     str: Or(
        #         And(str, shorthand_output_spec),
        #         {
        #             Required("name"): str,
        #             Optional("dest"): All(str, Match(r"^s3://", msg="output destination must be an s3 path")),
        #             Optional("s3_tags", default={}): {str: Coerce(str)},
        #         },
        #     ),
        # },
        Optional("outputs", default={}): file_list(output_spec),
        Exclusive("skip_if_output_exists", "skip_behavior", msg=skip_msg): bool,
        Exclusive("skip_on_rerun", "skip_behavior", msg=skip_msg): bool,
        Optional("compute", default={}): {
            Optional("consumes", default={}): {str: All(int, Range(min=1))},
            Optional("cpus", default=1): All(int, Range(min=1)),
            Optional("gpu", default=0): Or(
                All(int, Range(min=0)),
                "all",
                msg="gpu spec must be a nonnegative integer or 'all'"
            ),
            Optional("memory", default="1 Gb"): Any(float, int, str, msg="memory must be a number or string"),
            Optional("queue_name", default=None): Maybe(str),
            Optional("shell", default=None): Any(None, "bash", "sh", "sh-pipefail",
                                                 msg="shell option must be bash, sh, or sh-pipefail"),
            Optional("spot", default=True): bool,
        },
        Optional("filesystems", default=[]): listified(filesystem_block),
        Optional("qc_check", default=[]): listified(qc_check_block),
        Optional("retry", default={}): {
            Optional("attempts", default=3): int,
            Optional("backoff_rate", default=1.5): All(Any(float, int),
                                                       Clamp(min=1.0, msg="backoff rate must be at least 1.0")),
            Optional("interval", default="3s"): Match(r"^\d+\s?[smhdw]$",
                                                      msg="incorrect retry interval time string"),
        },
        Optional("timeout", default=None): Any(None, Match(r"^\d+\s?[smhdw]$",
                                                           msg="incorrect timeout time string")),
        **next_or_end,
    },
    no_shared_keys("inputs", "outputs", "references"),
))


native_step_schema = Schema(
    {
        Required("Type"):
            All(
                NotIn(["Choice", "Map"], msg="Choice and Map Types not supported"),
                Any("Pass", "Task", "Wait", "Succeed", "Fail", "Parallel",
                    msg="Type must be Pass, Task, Wait, Succeed, Fail, or Parallel")
            ),
        Extra: object,
    }
)


parallel_branch = {
    Optional("if"): str,
    Required("steps", msg="steps list not found"): listified(dict, min=1),
}

parallel_step_schema = Schema(
    {
        Optional("inputs", default={}): file_list({str: str}),
        Required("branches", msg="branches not found"): listified(parallel_branch, min=1),
        **next_or_end,
    }
)


choice = {
    Required("if", msg="no 'if' condition found"): str,
    Required("next", msg="no 'next' name found"): str,
}

chooser_step_schema = Schema(
    {
        Optional("inputs", default={}): file_list({str: str}),
        Required("choices", msg="choices list not found"): listified(choice, min=1),
        Optional("next"): str,
    }
)


scatter_step_schema = Schema(All(
    {
        Required("scatter"): {str: Any(str, list)},
        Optional("inputs", default=None): file_list(Any(None, {str: str})),
            ## remove  Maybe({str: str}),
        Required("steps", "steps list is required"): listified({str: dict}, min=1),
        Optional("outputs", default={}):
            {str: str},
        Optional("max_concurrency", default=0):
            All(Coerce(int), Range(min=0, msg="max_concurrency must be zero or greater")),
        Optional("error_tolerance", default=0): Or(All(int, Range(min=0)),          # integer >= 0
                                                   Match(r"^0*(?:\d{1,2}|100)%$"),  # percentage, 0 - 100%
                                                   msg="invalid error tolerance request"),
        **next_or_end,
    },
    # It's technically OK if scatter shares keys with these, because it's namespaced as ${scatter.foo}
    no_shared_keys("inputs", "outputs")
))


subpipe_step_schema = Schema(
    {
        Optional("job_data", default=None): Maybe(str),
        Optional("submit", default=[]): listified(str),  # deprecated
        Required("subpipe"): str,
        Optional("retrieve", default=[]): listified(str),
        **next_or_end,
    }
)


wf_step = {Coerce(str): dict}

workflow_schema = Schema(
    {
        Required("Repository", msg="Repository is required"): str,
        Optional("Parameters"): {
            str: {
                Required("Type", msg="Parameter Type is required"): str,
                Extra: object,
            },
        },
        Optional("Options", default={}): {
            Optional("shell", default="sh"): Any ("bash", "sh", "sh-pipefail",
                                                  msg="shell option must be bash, sh, or sh-pipefail"),
            Optional("task_role", default=None): Maybe(str),
            Optional("s3_tags", default={}): {str: Coerce(str)},
            Optional("job_tags", default={}): {str: Coerce(str)},
            # deprecated...
            Optional("versioned", default="false"): All(Lower, Coerce(str), Any("true", "false"))
        },
        Required("Steps", "Steps list not found"): listified(wf_step, min=1),
    }
)


def _validator(spec: dict, schema: Schema, where: str):
    try:
        ret = schema(spec)
        return ret
    except Invalid as inv:
        raise CompilerError(inv, where=where)


def validate_batch_step(step: Step) -> Step:
    normalized_spec = _validator(step.spec, batch_step_schema, f"batch job step '{step.name}'")
    return Step(step.name, normalized_spec, step.next)


def validate_native_step(step: Step) -> Step:
    normalized_spec = _validator(step.spec, native_step_schema, f"native step '{step.name}")
    return Step(step.name, normalized_spec, step.next)


def validate_parallel_step(step: Step) -> Step:
    normalized_spec = _validator(step.spec, parallel_step_schema, f"parallel step '{step.name}")
    return Step(step.name, normalized_spec, step.next)


def validate_scatter_step(step: Step) -> Step:
    normalized_spec = _validator(step.spec, scatter_step_schema, f"scatter/gather step '{step.name}'")
    return Step(step.name, normalized_spec, step.next)


def validate_subpipe_step(step: Step) -> Step:
    normalized_spec = _validator(step.spec, subpipe_step_schema, f"subpipe step '{step.name}'")
    return Step(step.name, normalized_spec, step.next)


def validate_chooser_step(step: Step) -> Step:
    normalized_spec = _validator(step.spec, chooser_step_schema, f"chooser step '{step.name}'")
    return Step(step.name, normalized_spec, step.next)

from contextlib import closing
import csv
import json
import re

import boto3
from jsonpath import jsonpath
import yaml

# matches:
#   s3://(bucket)/(key/key/key.ext):(jsonpath)
#   s3://(bucket)/(key/key/key.ext)
PARSER = re.compile(r"^s3://(.+?)/([^:]+)(?::(.+))?$")


def read_json(body):
    ret = json.load(body)
    return ret


def read_json_lines(body):
    ret = [json.loads(l) for l in body.iter_lines()]
    return ret


def read_yaml(body):
    ret = yaml.load(body, Loader=yaml.SafeLoader)
    return ret


def read_csv(body, delim=","):
    text = (l.decode("utf-8") for l in body.iter_lines())
    ret = list(csv.DictReader(text, delimiter=delim))
    return ret


def slurp(body):
    ret = [l.decode("utf-8") for l in body.iter_lines()]
    return ret


def stringify(item) -> str:
    if isinstance(item, (dict, list)):
        return json.dumps(item)
    else:
        return str(item)


def select_file_contents(s3_path: str) -> list:
    bucket, key, selector = PARSER.fullmatch(s3_path).groups()

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    with closing(response["Body"]) as fp:
        if selector is None:
            ret0 = slurp(fp)
        else:
            if key.endswith(".json"):
                contents = read_json(fp)
            elif key.endswith(".jsonl") or key.endswith(".ndjson"):
                contents = read_json_lines(fp)
            elif key.endswith(".yaml") or key.endswith(".yml"):
                contents = read_yaml(fp)
            elif key.endswith(".csv"):
                contents = read_csv(fp)
            elif key.endswith(".tsv") or key.endswith(".tab"):
                contents = read_csv(fp, delim="\t")
            else:
                contents = slurp(fp)

            ret0 = jsonpath(contents, selector)

    assert isinstance(ret0, list)
    ret = [stringify(i) for i in ret0]

    return ret

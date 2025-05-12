import re

import pytest

from ..src.runner.string_subs import lookup, substitute, substitute_image_tag


@pytest.mark.parametrize("pattern, string, expect", [
    (r"(one)", "one", "wun"),
    (r"(two)", "two", "2"),
    (r"(three)", "three", ""),
    (r"(four)", "four", "False"),
    (r"(not_found)", "not_found", "not_found")
])
def test_lookup(pattern, string, expect):
    spec = {
        "one": "wun",
        "two": 2,
        "three": "",
        "four": False,
    }
    match = re.match(pattern, string)
    result = lookup(match, spec)
    assert isinstance(result, str)
    assert result == expect


def test_substitute_string():
    subs = {
        "w": "was",
        "x": "am",
        "y": {
            "z": "very",
            "t": "singular",
        },
        "p": ["exemplar", {"what": "model"}, "blueprint"],
        "q": ["modern", "major"],
    }
    target = "I ${x} the ${y.z} ${p[1].what} of a ${q} ${general}"
    result = substitute(target, subs)
    expect = "I am the very model of a ['modern', 'major'] ${general}"
    assert result == expect


def test_substitute_nested():
    subs = {
        "metadata": {
            "received": "2022-06-01",
        },
    }
    target = "received on ${metadata.received}"
    result = substitute(target, subs)
    expect = "received on 2022-06-01"
    assert result == expect


def test_substitute_falsy_values():
    subs = {
        "job": {
            "boolean_T": True,
            "boolean_F": False,
            "null": None,
            "zero": 0,
            "empty_string": "",
        }
    }
    target = "command ${job.boolean_T} ${job.boolean_F} ${job.null} ${job.zero} <${job.empty_string}> ${job.not_found}"
    result = substitute(target, subs)
    expect = "command True False ${job.null} 0 <> ${job.not_found}"
    assert result == expect


def test_substitute_recursion():
    subs = {
        "a": 99,
        "b": "two",
    }
    target = {
        "one": [
            {
                "three": "${a}",
                "four": "${b}",
            },
            [
                "${a}",
                "${b}",
            ],
            "${a} ${b}",
        ],
        "two": {
            "seven": {
                "five": "${a}",
                "six": "${b}",
            },
            "eight": [
                "${a}",
                "${b}",
            ],
            "nine": "${a} ${b}"
        },
    }
    result = substitute(target, subs)
    expect = {
        "one": [
            {
                "three": "99",
                "four": "two",
            },
            [
                "99",
                "two",
            ],
            "99 two",
        ],
        "two": {
            "seven": {
                "five": "99",
                "six": "two",
            },
            "eight": [
                "99",
                "two",
            ],
            "nine": "99 two"
        },
    }
    assert result == expect


@pytest.mark.parametrize("original, expect", [
    ("docker.io/library/single:${sub}", "docker.io/library/single:tag"),
    ("no_${a}_registry:${sub}", "no_eh_registry:tag"),
    ("no_registry:no_subs", "no_registry:no_subs"),
    ("public.ecr.aws/docker/library/multi:${a}_${b}_${c}", "public.ecr.aws/docker/library/multi:eh_bee_sea"),
    ("123456789012.dkr.ecr.us-east-1.amazonaws.com/no:subs", "123456789012.dkr.ecr.us-east-1.amazonaws.com/no:subs"),
    ("123456789012.dkr.ecr.us-east-1.amazonaws.com/no_tags", "123456789012.dkr.ecr.us-east-1.amazonaws.com/no_tags"),
    ("myregistryhost:5000/fedora/httpd:${sub}", "myregistryhost:5000/fedora/httpd:tag"),  # https://docs.docker.com/engine/reference/commandline/tag/#tag-an-image-for-a-private-repository
    ("probably:${a}/highly/${b}/illegal/${c}:${sub}", "probably:${a}/highly/${b}/illegal/sea:tag"),
])
def test_substitute_image_tag(original, expect):
    spec = {
        "sub": "tag",
        "a": "eh",
        "b": "bee",
        "c": "sea",
    }
    result = substitute_image_tag(original, spec)
    assert result == expect


def test_substitute_tagged_output():
    output_spec = {
        "name": "fake_${a}_filename",
        "s3_tags": {
            "tag1": "value_${a}",
            "tag2": "value_${b}",
        }
    }

    subs = {
        "a": 99,
        "b": "two",
    }

    expect = {
        "name": "fake_99_filename",
        "s3_tags": {
            "tag1": "value_99",
            "tag2": "value_two",
        }
    }

    result = substitute(output_spec, subs)
    assert result == expect

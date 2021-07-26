import re

import pytest

from ..src.runner.string_subs import lookup, substitute


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

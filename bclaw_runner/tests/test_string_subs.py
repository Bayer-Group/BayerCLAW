from ..src.runner.string_subs import substitute


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

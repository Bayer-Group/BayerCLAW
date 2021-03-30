import logging

from ..src.runner.runnit import runnit


def test_runnit(caplog, read_config):
    shell = read_config["shell-exe"]
    cmd = [shell, "-c", "echo 'stdout'; >&2 echo 'stderr'; sleep 2; date +%s"]

    with caplog.at_level(logging.INFO):
        result = runnit(cmd)
        assert result == 0

        assert set(caplog.messages[:2]) == {"stdout", "stderr"}

        # check that the subprocess stdout & stderr were logged in real time
        stop_time = float(caplog.messages[2])
        assert caplog.records[0].created < stop_time
        assert caplog.records[1].created < stop_time


def test_runnit_fail(caplog, read_config):
    shell = read_config["shell-exe"]
    cmd = [shell, "-c", "false"]

    with caplog.at_level(logging.INFO):
        result = runnit(cmd)
        assert result > 0


def test_runnit_read_stream_error(caplog, read_config):
    shell = read_config["shell-exe"]
    cmd = [shell, "-c", "echo antidisestablishmentarianism"]

    with caplog.at_level(logging.INFO):
        result = runnit(cmd, stream_limit=20)
        assert result == 0
    assert "error reading process stdout/stderr" in caplog.text

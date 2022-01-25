import os
import pytest
import signal
import threading
import time

from ..src.runner.signal_trapper import signal_trapper

# def test_signal_handler():
#     with pytest.raises(SignalTrapped) as st:
#         signal_handler(1, "unused")
#     assert "received signal Hangup" in str(st)


def test_signal_trapper(mock_container_factory):
    pid = os.getpid()

    def trigger_signal():
        time.sleep(1)
        os.kill(pid, 2)

    thread = threading.Thread(target=trigger_signal)
    thread.daemon = True
    thread.start()

    test_container = mock_container_factory(0, False)

    with signal_trapper(test_container):
        time.sleep(3)
        print("yo")

    assert test_container.exit_code == 99  # test_container.stop() was called

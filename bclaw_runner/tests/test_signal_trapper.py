import os
import pytest
import signal
import threading
import time

from ..src.runner.signal_trapper import SignalTrapped, signal_handler, signal_trapper

def test_signal_handler():
    with pytest.raises(SignalTrapped) as st:
        signal_handler(1, "unused")
    assert "received signal Hangup: 1" in str(st)


def test_signal_trapper():
    pid = os.getpid()

    def trigger_signal():
        time.sleep(1)
        os.kill(pid, 2)

    thread = threading.Thread(target=trigger_signal)
    thread.daemon = True
    thread.start()

    with pytest.raises(SignalTrapped) as st:
        with signal_trapper():
            time.sleep(5)
            print("yo")

    assert "received signal Interrupt: 2" in str(st)
    assert signal.getsignal(1) == signal.SIG_DFL

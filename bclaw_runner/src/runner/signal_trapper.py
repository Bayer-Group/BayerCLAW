from contextlib import contextmanager
import logging
import signal

from docker.models.containers import Container

logger = logging.getLogger(__name__)


# https://stackoverflow.com/questions/2148888/python-trap-all-signals
@contextmanager
def signal_trapper(container: Container):
    def _handler(signal_number: int, _):
        logger.warning(f"received signal {signal.strsignal(signal_number)}")
        logger.warning("stopping subprocess")
        container.stop(timeout=5)

    original_handlers = {}
    try:
        logger.debug("setting new signal handlers")
        for sig in signal.valid_signals() - {signal.SIGKILL, signal.SIGSTOP}:
            if signal.getsignal(sig) is not signal.SIG_IGN:
                original_handlers[sig] = signal.signal(sig, _handler)
        yield
    finally:
        logger.debug("restoring signal handlers")
        for k, v in original_handlers.items():
            signal.signal(k, v)

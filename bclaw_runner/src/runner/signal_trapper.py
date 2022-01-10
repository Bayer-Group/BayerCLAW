from contextlib import contextmanager
import logging
import signal

logger = logging.getLogger(__name__)


class SignalTrapped(Exception):
    def __init__(self, signal_number: int):
        msg = f"received signal {signal.strsignal(signal_number)}; exiting"
        super().__init__(msg)


def signal_handler(signal_number: int, _) -> None:
    logger.debug(f"trapped signal {signal_number}")
    raise SignalTrapped(signal_number)


# https://stackoverflow.com/questions/2148888/python-trap-all-signals
@contextmanager
def signal_trapper():
    original_handlers = {}
    try:
        logger.debug("setting new signal handlers")
        for sig in signal.valid_signals() - {signal.SIGKILL, signal.SIGSTOP}:
            if signal.getsignal(sig) is not signal.SIG_IGN:
                original_handlers[sig] = signal.signal(sig, signal_handler)
        yield
    except:
        raise
    finally:
        logger.debug("restoring signal handlers")
        for k, v in original_handlers.items():
            signal.signal(k, v)


if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.INFO)
    with signal_trapper():
        try:
            time.sleep(15)
        except BaseException:
            logger.error("yada yada")
            raise
    print("woohoo")

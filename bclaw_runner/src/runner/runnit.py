import asyncio
from contextlib import closing
from functools import partial
import logging

# https://kevinmccarthy.org/2016/07/25/streaming-subprocess-stdin-and-stdout-with-asyncio-in-python/


async def _read_stream(stream, callback):
    while True:
        try:
            line = await stream.readline()
        except Exception as e:
            line = f"error reading process stdout/stderr: {str(e)}".encode("utf-8")

        if line:
            callback(line.decode("utf-8").rstrip())
        else:
            break


async def _stream_subprocess(command, env, stdout_callback, stderr_callback, stream_limit):
    process = await asyncio.create_subprocess_exec(*command,
                                                   env=env,
                                                   stdout=asyncio.subprocess.PIPE,
                                                   stderr=asyncio.subprocess.PIPE,
                                                   limit=stream_limit)
    await asyncio.wait([
        _read_stream(process.stdout, stdout_callback),
        _read_stream(process.stderr, stderr_callback)
    ])

    return await process.wait()


def runnit(cmd, env=None, out_fp=None, err_fp=None, logger=None, stream_limit=asyncio.streams._DEFAULT_LIMIT):
    if logger is None:
        logger = logging.getLogger(__name__)
    else:
        logger = logger.getChild("runnit")

    out_callback = logger.info if out_fp is None else partial(print, file=out_fp)
    err_callback = logger.info if err_fp is None else partial(print, file=err_fp)

    # http://stackoverflow.com/questions/37778019/aiohttp-asyncio-runtimeerror-event-loop-is-closed
    with closing(asyncio.new_event_loop()) as loop:
        asyncio.set_event_loop(loop)
        rc = loop.run_until_complete(
            _stream_subprocess(cmd, env, out_callback, err_callback, stream_limit)
        )
        return rc

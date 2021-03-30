from concurrent.futures import ThreadPoolExecutor
import fcntl
import logging
import os
from typing import Dict, Tuple

import backoff
import boto3


logger = logging.getLogger(__name__)
logging.getLogger("backoff").setLevel(logging.ERROR)


def _backoff_handler(details):
    name = details["kwargs"]["name_for_logging"]
    wait = details["wait"]
    logger.debug(f"failed to lock {name}, retrying in {wait} seconds")


def _blocking_download(s3_object, dest_path: str, name_for_logging: str) -> None:
    if os.path.isfile(dest_path):
        logger.info(f"found {name_for_logging} in cache")
    else:
        logger.debug(f"acquiring a lock on {name_for_logging}")
        lock_path = f"{os.path.dirname(dest_path)}.lock"
        with open(lock_path, "w") as lfp:
            fcntl.flock(lfp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            logger.debug(f"lock acquired for {name_for_logging}")
            logger.info(f"downloading {name_for_logging} to cache")
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            s3_object.download_file(dest_path)
            logger.info(f"{name_for_logging} downloaded to cache")
            logger.debug(f"releasing lock on {name_for_logging}")
            fcntl.flock(lfp, fcntl.LOCK_UN)
        os.remove(lock_path)


@backoff.on_exception(backoff.constant,
                      BlockingIOError,
                      interval=5,
                      jitter=None,
                      on_backoff=_backoff_handler)
def _download_loop(s3_object, dest_path: str, *, name_for_logging: str) -> None:
    _blocking_download(s3_object, dest_path, name_for_logging)


def _download_to_cache(item: Tuple[str, str]) -> Tuple[str, str]:
    session = boto3.Session()

    key, s3_path = item
    s3_bucket, s3_key = s3_path.split("/", 3)[2:]
    src = session.resource("s3").Object(s3_bucket, s3_key)

    cache_path = os.environ["BC_SCRATCH_PATH"]
    src_etag = src.e_tag.strip('"')  # ETag comes wrapped in double quotes for some reason
    file_name = os.path.basename(s3_key)

    cached_file = f"{cache_path}/{src_etag}/{file_name}"

    _download_loop(src, cached_file, name_for_logging=file_name)

    return key, cached_file


def get_reference_inputs(ref_spec: Dict[str, str]) -> Dict[str, str]:
    ret = {}

    if len(ref_spec) > 0:
        logger.info(f"caching references: {list(ref_spec.values())}")

        with ThreadPoolExecutor(max_workers=len(ref_spec)) as executor:
            result = list(executor.map(_download_to_cache, ref_spec.items()))

        for key, src in result:
            dst = ret[key] = os.path.basename(src)
            logger.info(f"linking cached {dst} to workspace")
            os.link(src, dst)

    return ret

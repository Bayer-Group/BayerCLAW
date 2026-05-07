from concurrent.futures import ThreadPoolExecutor
import glob as g
import logging
import os
import re
from typing import Generator, Tuple

import boto3

logger = logging.getLogger(__name__)


def _exporterator(output_specs: list[str]) -> Generator[Tuple[str, str], None, None]:
    for spec in output_specs:
        src, dest = re.split(r"\s+->\s+", spec, maxsplit=1)
        expanded = g.glob(src, recursive=True)
        if not expanded:
            logger.warning(f"no file matching '{src}' found in workspace")
        for filename in expanded:
            yield filename, dest


def _export_that(src: str, dst: str) -> str:
    local_size = os.path.getsize(src)

    if dst.endswith("/"):
        dst += os.path.basename(src)
    bucket, key = dst.split("/", 3)[2:]

    logger.info(f"uploading {src} to s3://{bucket}/{key}")
    logger.info(f"starting upload: {src} ({local_size} bytes) -> {dst}")

    session = boto3.Session()
    s3 = session.resource("s3")
    s3_obj = s3.Object(bucket, key)
    s3_obj.upload(src, ExtraArgs={"ServerSideEncryption": "AES256"})
    s3_size = s3_obj.content_length

    logger.info(f"finished upload: {src} ({local_size} bytes) -> {dst} ({s3_size} bytes)")

    return dst


def do_exports(output_specs: list[str]) -> None:
    exporter = lambda src_dst: _export_that(*src_dst)

    with ThreadPoolExecutor(max_workers=10) as executor:
        result = list(executor.map(exporter, _exporterator(output_specs)))
    logger.info(f"{len(result)} files uploaded")

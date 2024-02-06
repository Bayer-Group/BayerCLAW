FROM public.ecr.aws/docker/library/python:3.10-alpine

LABEL maintainer="jack.tabaska@bayer.com"

# This was copied from ../Dockerfile.alpine and edited appropriately for development

RUN pip install --no-cache-dir --upgrade pip && \
    apk add --no-cache --virtual .build-deps \
    build-base openssl-dev libffi-dev && \
    pip install --no-cache-dir pytest moto requests_mock

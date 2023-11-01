FROM public.ecr.aws/docker/library/python:3.10-slim AS base

LABEL maintainer="jack.tabaska@bayer.com"

# https://www.cynnovative.com/simple-multi-stage-docker-builds/

WORKDIR /bclaw_runner

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY src src
COPY __init__.py __init__.py

FROM base AS test

RUN pip install --no-cache-dir pytest moto requests_mock

COPY tests tests
RUN pytest -s -vvv tests/

FROM base AS build

ENV PYTHONBUFFERED=1
ENV PATH=/bclaw:$PATH

FROM python:3.9.6-slim-bullseye AS base

LABEL maintainer="jack.tabaska@bayer.com"

# https://www.cynnovative.com/simple-multi-stage-docker-builds/

WORKDIR /bclaw_runner

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY src src
COPY __init__.py __init__.py

FROM base AS test

RUN pip install pytest moto requests_mock

COPY tests tests
RUN pytest -s -vvv tests/

FROM base AS build

ENV PATH=/bclaw:$PATH

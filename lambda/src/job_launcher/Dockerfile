FROM public.ecr.aws/lambda/python:3.9 AS base

LABEL maintainer="jack.tabaska@bayer.com"

# RUN yum update -y && \
#     yum update -y krb5 && \
#     yum update -y libpng && \
#     yum update -y libtasn1 && \
#     yum update -y sqlite && \
#     yum -y clean all && \
#     rm -rf /var/cache/yum

WORKDIR ${LAMBDA_TASK_ROOT}

COPY src/job_launcher/requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt --target ${LAMBDA_TASK_ROOT}

COPY src/job_launcher/job_launcher.py ${LAMBDA_TASK_ROOT}
COPY src/common/python/lambda_logs.py ${LAMBDA_TASK_ROOT}

ENV PYTHONPATH=/opt/lambda/src/job_launcher

#COPY src/job_launcher/ /opt/src/job_launcher/

COPY src/job_launcher/pkg /opt/lambda/src/job_launcher/pkg/
COPY src/job_launcher/__init__.py /opt/lambda/src/job_launcher/
COPY src/__init__.py /opt/lambda/src/
COPY __init__.py /opt/lambda/

#CMD ["job_launcher.lambda_handler"]

FROM base AS test

RUN pip install --no-cache-dir pytest moto[all]==3.1.3

COPY tests/job_launcher/ /opt/lambda/tests/job_launcher/
COPY tests/__init__.py /opt/lambda/tests/

RUN AWS_DEFAULT_REGION=us-east-1 pytest -s -vvv /opt/lambda/tests/

FROM base AS build

WORKDIR ${LAMBDA_TASK_ROOT}

CMD ["job_launcher.lambda_handler"]

FROM public.ecr.aws/lambda/python:3.9

LABEL maintainer="jack.tabaska@bayer.com"

WORKDIR ${LAMBDA_TASK_ROOT}

COPY src/job_launcher/requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt --target ${LAMBDA_TASK_ROOT}
COPY src/job_launcher/job_launcher.py ${LAMBDA_TASK_ROOT}
COPY src/common/python/lambda_logs.py ${LAMBDA_TASK_ROOT}

# todo: need this?
#ENV PYTHONPATH=/opt/python

CMD ["job_launcher.lambda_handler"]

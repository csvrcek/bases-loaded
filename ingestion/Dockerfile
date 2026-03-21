FROM public.ecr.aws/lambda/python:3.12

ARG SCRAPER

COPY ingestion/${SCRAPER}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ${LAMBDA_TASK_ROOT}/ingestion/

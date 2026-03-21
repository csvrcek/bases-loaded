FROM public.ecr.aws/lambda/python:3.12

ARG SCRAPER

COPY ${SCRAPER}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ${LAMBDA_TASK_ROOT}/ingestion/

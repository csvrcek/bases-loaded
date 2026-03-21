FROM public.ecr.aws/lambda/python:3.12

COPY infra/runtime/weather_scraper/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ${LAMBDA_TASK_ROOT}/ingestion/
COPY infra/runtime/weather_scraper/index.py ${LAMBDA_TASK_ROOT}/

CMD ["index.handler"]

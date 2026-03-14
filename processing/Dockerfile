FROM public.ecr.aws/lambda/python:3.12

COPY processing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ${LAMBDA_TASK_ROOT}/shared/
COPY processing/ ${LAMBDA_TASK_ROOT}/processing/

CMD ["processing.handler.lambda_handler"]

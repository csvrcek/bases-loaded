FROM public.ecr.aws/lambda/python:3.12

COPY inference/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ${LAMBDA_TASK_ROOT}/shared/
COPY ml/ ${LAMBDA_TASK_ROOT}/ml/
COPY inference/ ${LAMBDA_TASK_ROOT}/inference/

CMD ["inference.predict.handler.handler"]

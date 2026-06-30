FROM python:3.12-slim

WORKDIR /app

COPY cdc/consumer ./cdc/consumer

RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    kafka-python \
    pyarrow

CMD ["python", "-m", "cdc.consumer.consumer"]

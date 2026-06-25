FROM python:3.12-slim

WORKDIR /app

COPY src/consumer ./src/consumer

RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    kafka-python \
    pyarrow

CMD ["python", "-m", "src.consumer.consumer"]

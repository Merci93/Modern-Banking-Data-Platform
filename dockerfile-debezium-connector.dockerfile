FROM python:3.12-slim

WORKDIR /app

COPY cdc/debezium ./cdc/debezium

RUN pip install --no-cache-dir requests

CMD ["python", "-m", "cdc.debezium.debezium_connector"]

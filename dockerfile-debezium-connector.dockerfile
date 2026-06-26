FROM python:3.12-slim

WORKDIR /app

COPY src/debezium/debezium_connector.py .

RUN pip install --no-cache-dir requests python-dotenv

CMD ["python", "debezium_connector.py"]

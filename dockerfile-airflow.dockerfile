# Docker file to build airflow image
FROM apache/airflow:2.9.3

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
    -r /tmp/requirements.txt

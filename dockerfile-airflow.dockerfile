# Docker file to build airflow image with dbt installed
FROM apache/airflow:2.9.3

USER airflow

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

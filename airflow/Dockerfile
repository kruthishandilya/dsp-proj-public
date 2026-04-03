FROM apache/airflow:3.0.2

USER root
RUN apt-get update && apt-get install -y curl

USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

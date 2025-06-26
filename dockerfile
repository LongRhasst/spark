FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && apt-get install -y build-essential

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt -c $CONSTRAINT_URL

COPY dags /opt/airflow/dags

FROM apache/airflow:2.9.3-python3.10

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt -c $CONSTRAINT_URL

USER root
RUN apt-get update && apt-get install -y build-essential default-jdk procps

# Set SPARK_HOME and JAVA_HOME to point to correct locations
ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark
ENV JAVA_HOME=/usr/lib/jvm/default-java

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY dags /opt/airflow/dags

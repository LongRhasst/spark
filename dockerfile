FROM apache/airflow:2.9.3-python3.10

USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt -c $CONSTRAINT_URL

USER root
RUN apt-get update && apt-get install -y build-essential default-jdk procps

# Set SPARK_HOME and JAVA_HOME to point to correct locations
ENV SPARK_HOME=opt/bitnami/spark
ENV JAVA_HOME=/usr/lib/jvm/default-java

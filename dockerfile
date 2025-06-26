FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && apt-get install -y build-essential

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY dags /opt/airflow/dags
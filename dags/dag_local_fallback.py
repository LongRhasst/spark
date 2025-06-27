from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sys
import os

# Add the airflow directory to Python path
sys.path.append('/opt/airflow')

# Import with error handling
try:
    from include.scripts.request_data import request_data
    print("Successfully imported request_data")
except ImportError as e:
    print(f"Failed to import request_data: {e}")
    # Define a dummy function as fallback
    def request_data(url):
        print(f"Dummy request_data called with URL: {url}")
        return "Dummy data"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_processing_pipeline_local',
    default_args=default_args,
    description='A DAG to ingest, structure, and transform data using Spark in LOCAL mode',
    schedule_interval='8 0 * * *',
    catchup=False,
    tags=['spark', 'data_pipeline', 'local_mode'],
) as dag:
    
    t1 = PythonOperator(
        task_id='Ingest_Data_From_API',
        python_callable=request_data,
        op_kwargs={'url':'https://restcountries.com/v3.1/independent?status=true'}
    )
    
    # Define paths consistently with docker-compose volume mapping
    raw_data_path = '/opt/airflow/data/raw/raw.json'
    foundation_data_path = '/opt/airflow/data/foundation'
    trusted_data_path = '/opt/airflow/data/trusted'

    # Task 2: Use LOCAL mode instead of YARN
    t2 = SparkSubmitOperator(
        task_id='Structure_Data_With_Spark_Local',
        application='/opt/airflow/include/scripts/structured_data.py',
        verbose=True,
        # Use local mode with optimized configuration
        conf={
            'spark.master': 'local[*]',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.driver.maxResultSize': '1g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '60s',
            'spark.sql.warehouse.dir': '/tmp/spark-warehouse',
            'spark.hadoop.fs.defaultFS': 'file:///'
        },
        application_args=[raw_data_path, foundation_data_path],
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )
    
    t3 = SparkSubmitOperator(
        task_id='Transform_Data_With_Spark_Local',
        application='/opt/airflow/include/scripts/transform_data.py',
        verbose=True,
        # Use local mode
        conf={
            'spark.master': 'local[*]',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.driver.maxResultSize': '1g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '60s',
            'spark.sql.warehouse.dir': '/tmp/spark-warehouse',
            'spark.hadoop.fs.defaultFS': 'file:///'
        },
        application_args=[foundation_data_path, trusted_data_path],
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )

    t4 = SparkSubmitOperator(
        task_id='Save_Data_To_MySQL_Local',
        application='/opt/airflow/include/scripts/save.py',
        verbose=True,
        # Use local mode with MySQL connector
        packages='mysql:mysql-connector-java:8.0.28',
        conf={
            'spark.master': 'local[*]',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.driver.maxResultSize': '1g',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '60s',
            'spark.sql.warehouse.dir': '/tmp/spark-warehouse',
            'spark.hadoop.fs.defaultFS': 'file:///'
        },
        application_args=[trusted_data_path, 'countries'],
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )

    t1 >> t2 >> t3 >> t4
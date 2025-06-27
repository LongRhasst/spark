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
    'start_date': datetime(2023, 10, 1), # Consider using a dynamic start_date for new DAGs e.g., days_ago(1)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_processing_pipeline', # More descriptive dag_id
    default_args=default_args,
    description='A DAG to ingest, structure, and transform data using Spark',
    schedule_interval= '8 0 * * *',
    catchup=False,
    tags=['spark', 'data_pipeline'], # More descriptive tags
) as dag:
    
    t1 = PythonOperator(
        task_id='Ingest_Data_From_API', # More descriptive task_id
        python_callable=request_data,
        op_kwargs={'url':'https://restcountries.com/v3.1/independent?status=true'}
    )
    
    # Define paths consistently with docker-compose volume mapping
    raw_data_path = '/opt/airflow/data/raw/raw.json'
    foundation_data_path = '/opt/airflow/data/foundation'
    trusted_data_path = '/opt/airflow/data/trusted'

    # For task 2, use a direct approach with command-line arguments
    t2 = SparkSubmitOperator(
        task_id='Structure_Data_With_Spark',
        application='/opt/airflow/include/scripts/structured_data.py',
        conn_id='spark_default',
        verbose=True,
        # Use standalone Spark cluster
        conf={"spark.master": "spark://spark-master:7077"},
        application_args=[raw_data_path, foundation_data_path],
        # Explicitly set the spark-submit binary path
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )
    
    t3 = SparkSubmitOperator(
        task_id='Transform_Data_With_Spark', # Corrected typo and more descriptive task_id
        application='/opt/airflow/include/scripts/transform_data.py', # Updated path
        conn_id='spark_default',
        verbose=True,
        # Use standalone Spark cluster
        conf={"spark.master": "spark://spark-master:7077"},
        application_args=[foundation_data_path, trusted_data_path], # Pass the input path to the script
        # Explicitly set the spark-submit binary path
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )

    t4 = SparkSubmitOperator(
        task_id='Save_Data_To_MySQL', # More descriptive task_id
        application='/opt/airflow/include/scripts/save.py', # Updated path
        conn_id='spark_default',
        verbose=True,
        # Use standalone Spark cluster and MySQL connector
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.jars.packages": "mysql:mysql-connector-java:8.0.28"
        },
        application_args=[trusted_data_path, 'countries'], # Pass the input path and table name
        # Explicitly set the spark-submit binary path
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )

    t1 >> t2 >> t3 >> t4 # Ensure the order of execution is maintained
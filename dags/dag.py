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
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': None,
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
    
    t2 = SparkSubmitOperator(
        task_id="test_spark_connection", # More descriptive task_id
        application='/opt/airflow/include/scripts/test_spark_connection.py',
        conn_id='spark_default', # Use the Spark connection defined in Airflow
        conf={
            'spark.master': 'spark://spark-master:7077', # Use the Spark master URL from docker-compose
            'spark.driver.memory': '512m', # Reduced driver memory
            'spark.executor.memory': '512m', # Reduced executor memory
            'spark.executor.cores': '1', # Set number of cores per executor
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        },
        executor_cores=1,
        executor_memory='512m',
        driver_memory='512m',
        deploy_mode='client',
        execution_timeout=timedelta(minutes=8),
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    # Define paths consistently with docker-compose volume mapping
    raw_data_path = '/opt/airflow/data/raw/raw.json'
    foundation_data_path = '/opt/airflow/data/foundation'
    # For task 3, use Spark cluster mode with improved configuration
    t3 = SparkSubmitOperator(
        task_id='Structure_Data_With_Spark',
        application='/opt/airflow/include/scripts/structured_data.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077', # Use the Spark master URL from docker-compose
            'spark.driver.memory': '512m', # Reduced driver memory
            'spark.executor.memory': '512m', # Reduced executor memory
            'spark.executor.cores': '1', # Set number of cores per executor
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        },
        executor_cores=1,
        executor_memory='512m',
        driver_memory='512m',
        deploy_mode='client',
        execution_timeout=timedelta(minutes=8),
        retries=2,
        retry_delay=timedelta(minutes=1),
        application_args=[raw_data_path, foundation_data_path], # Pass the input path to the script
    )
    t1 >> t3 # Ensure the order of execution is maintained
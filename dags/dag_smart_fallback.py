from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os
import subprocess

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

def check_yarn_availability(**context):
    """Check if YARN ResourceManager is available"""
    try:
        # Try to ping YARN ResourceManager
        result = subprocess.run(['curl', '-s', '-f', 'http://resourcemanager:8088/ws/v1/cluster/info'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ YARN ResourceManager is available")
            return 'yarn_mode_tasks'
        else:
            print("⚠️ YARN ResourceManager is not available")
            return 'local_mode_tasks'
    except Exception as e:
        print(f"❌ Error checking YARN availability: {e}")
        return 'local_mode_tasks'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spark_processing_pipeline_smart',
    default_args=default_args,
    description='A DAG that automatically chooses between YARN and local mode',
    schedule_interval='8 0 * * *',
    catchup=False,
    tags=['spark', 'data_pipeline', 'smart_mode'],
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

    # Branch operator to decide between YARN and local mode
    mode_checker = BranchPythonOperator(
        task_id='check_mode',
        python_callable=check_yarn_availability,
        provide_context=True
    )

    # Dummy operators for branching
    yarn_mode_start = DummyOperator(task_id='yarn_mode_tasks')
    local_mode_start = DummyOperator(task_id='local_mode_tasks')

    # YARN Mode Tasks
    t2_yarn = SparkSubmitOperator(
        task_id='Structure_Data_With_Spark_YARN',
        application='/opt/airflow/include/scripts/structured_data.py',
        verbose=True,
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'client',
            'spark.yarn.appMasterEnv.YARN_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.yarn.appMasterEnv.HADOOP_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.executorEnv.YARN_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.executorEnv.HADOOP_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.yarn.queue': 'root.default',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '60s'
        },
        application_args=[raw_data_path, foundation_data_path],
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )
    
    t3_yarn = SparkSubmitOperator(
        task_id='Transform_Data_With_Spark_YARN',
        application='/opt/airflow/include/scripts/transform_data.py',
        verbose=True,
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'client',
            'spark.yarn.appMasterEnv.YARN_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.yarn.appMasterEnv.HADOOP_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.executorEnv.YARN_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.executorEnv.HADOOP_CONF_DIR': '/opt/hadoop/etc/hadoop',
            'spark.yarn.queue': 'root.default',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.network.timeout': '800s',
            'spark.executor.heartbeatInterval': '60s'
        },
        application_args=[foundation_data_path, trusted_data_path],
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit'
    )

    # Local Mode Tasks
    t2_local = SparkSubmitOperator(
        task_id='Structure_Data_With_Spark_Local',
        application='/opt/airflow/include/scripts/structured_data.py',
        verbose=True,
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
    
    t3_local = SparkSubmitOperator(
        task_id='Transform_Data_With_Spark_Local',
        application='/opt/airflow/include/scripts/transform_data.py',
        verbose=True,
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

    # Convergence point
    t4 = SparkSubmitOperator(
        task_id='Save_Data_To_MySQL',
        application='/opt/airflow/include/scripts/save.py',
        verbose=True,
        packages='mysql:mysql-connector-java:8.0.28',
        conf={
            'spark.master': 'local[*]',  # Always use local for MySQL save
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
        spark_binary='/home/airflow/.local/lib/python3.10/site-packages/pyspark/bin/spark-submit',
        trigger_rule='none_failed_or_skipped'  # Run if any upstream task succeeds
    )

    # Define the DAG structure
    t1 >> mode_checker
    mode_checker >> [yarn_mode_start, local_mode_start]
    
    # YARN path
    yarn_mode_start >> t2_yarn >> t3_yarn >> t4
    
    # Local path
    local_mode_start >> t2_local >> t3_local >> t4

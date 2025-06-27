from pyspark.sql import SparkSession
import sys
from sqlalchemy import create_engine
import os

# Get MySQL connection parameters from environment variables or use defaults
mysql_user = os.environ.get('MYSQL_USER', 'root')
mysql_password = os.environ.get('MYSQL_PASSWORD', 'password')
mysql_host = os.environ.get('MYSQL_HOST', 'mysql')
mysql_port = os.environ.get('MYSQL_PORT', '3306')
mysql_database = os.environ.get('MYSQL_DATABASE', 'airflow')

engine = create_engine(f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}")

def save_to_mysql(data, table_name):
    # Use the same environment variables for consistency
    mysql_user = os.environ.get('MYSQL_USER', 'root')
    mysql_password = os.environ.get('MYSQL_PASSWORD', 'password')
    mysql_host = os.environ.get('MYSQL_HOST', 'mysql')
    mysql_port = os.environ.get('MYSQL_PORT', '3306')
    mysql_database = os.environ.get('MYSQL_DATABASE', 'airflow')
    
    data.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}") \
        .option("dbtable", table_name) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()
        
if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        table_name = sys.argv[2] if len(sys.argv) > 2 else 'countries'
        
        spark = SparkSession.builder \
            .appName("SaveToMySQL") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        data = spark.read.parquet(input_path)
        
        save_to_mysql(data, table_name)
    else:
        print("Usage: save.py <input_path> [table_name]")
        # Example for local testing:
        # save_to_mysql('./foundation', 'countries')
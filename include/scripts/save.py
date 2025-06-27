from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import to_json, col
import sys
from sqlalchemy import create_engine
import os

def check_environment():
    """Check if required environment variables and paths are available"""
    print("🔍 Checking environment...")
    
    # Check Java
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        print(f"✅ JAVA_HOME: {java_home}")
    else:
        print("⚠️  JAVA_HOME not set")
    
    # Check Spark
    spark_home = os.environ.get('SPARK_HOME')
    if spark_home:
        print(f"✅ SPARK_HOME: {spark_home}")
    else:
        print("⚠️  SPARK_HOME not set")
    
    # Check Python path
    print(f"🐍 Python executable: {sys.executable}")
    print(f"📁 Current working directory: {os.getcwd()}")
    
    return True

# Get MySQL connection parameters from environment variables or use defaults
mysql_user = os.environ.get('MYSQL_USER', 'airflow')
mysql_password = os.environ.get('MYSQL_PASSWORD', 'airflow')
mysql_host = os.environ.get('MYSQL_HOST', 'mysql')
mysql_port = os.environ.get('MYSQL_PORT', '3306')
mysql_database = os.environ.get('MYSQL_DATABASE', 'airflow')

def save_to_mysql(data, table_name):
    """Save Spark DataFrame to MySQL with error handling and data validation"""
    try:
        # Use the same environment variables for consistency
        mysql_user = os.environ.get('MYSQL_USER', 'airflow')
        mysql_password = os.environ.get('MYSQL_PASSWORD', 'airflow')
        mysql_host = os.environ.get('MYSQL_HOST', 'mysql')
        mysql_port = os.environ.get('MYSQL_PORT', '3306')
        mysql_database = os.environ.get('MYSQL_DATABASE', 'airflow')
        
        print(f"🔗 Connecting to MySQL: {mysql_host}:{mysql_port}/{mysql_database}")
        print(f"👤 Using user: {mysql_user}")
        
        # Check data schema before saving
        print("📋 Data schema validation:")
        data.printSchema()
        
        # Check for any remaining complex types
        schema = data.schema
        complex_columns = []
        for field in schema.fields:
            if isinstance(field.dataType, (types.StructType, types.ArrayType, types.MapType)):
                complex_columns.append(field.name)
        
        if complex_columns:
            print(f"⚠️  Warning: Found complex columns that may cause issues: {complex_columns}")
            print("🔧 Attempting to handle complex columns...")
            
            # Try to convert remaining complex columns to JSON strings
            for col_name in complex_columns:
                print(f"📋 Converting {col_name} to JSON string...")
                data = data.withColumn(col_name, to_json(col(col_name)))
        
        # Validate data count
        record_count = data.count()
        print(f"📊 Total records to save: {record_count}")
        
        if record_count == 0:
            print("⚠️  Warning: No data to save!")
            return
        
        # Show sample data for verification
        print("📊 Sample data (first 2 rows):")
        data.show(2, truncate=True)
        
        # Save to MySQL with enhanced options
        print(f"💾 Saving to MySQL table: {table_name}")
        data.write \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}") \
            .option("dbtable", table_name) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci") \
            .option("batchsize", "1000") \
            .option("isolationLevel", "NONE") \
            .mode("overwrite") \
            .save()
            
        print(f"✅ Successfully saved {record_count} records to table '{table_name}'")
        
    except Exception as e:
        print(f"❌ Failed to save data to MySQL: {e}")
        print("🔍 Troubleshooting tips:")
        print("   - Check if MySQL container is running: docker-compose ps mysql")
        print("   - Verify MySQL credentials in .env file")
        print("   - Ensure MySQL connector JAR is available")
        print("   - Check if table schema is compatible with data types")
        print("   - Verify network connectivity between containers")
        
        # Print more detailed error information
        import traceback
        traceback.print_exc()
        raise e

def create_spark_session():
    """Create Spark session with fallback mechanism"""
    spark = None
    
    try:
        # Check if we're in a Docker/distributed environment
        is_distributed = os.environ.get('YARN_CONF_DIR') or os.environ.get('HADOOP_CONF_DIR')
        
        if is_distributed:
            print("🔧 Attempting Spark configuration for distributed environment...")
            try:
                # Try YARN first
                builder = SparkSession.builder \
                    .appName("SaveToMySQL") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.network.timeout", "800s") \
                    .config("spark.executor.heartbeatInterval", "60s")
                
                spark = builder.getOrCreate()
                print("✅ Successfully connected to YARN cluster")
                
            except Exception as yarn_error:
                print(f"⚠️  YARN connection failed: {yarn_error}")
                print("🔄 Falling back to local mode...")
                is_distributed = False
                
        if not is_distributed:
            print("🔧 Configuring Spark for local environment...")
            builder = SparkSession.builder \
                .appName("SaveToMySQL") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
                .config("spark.hadoop.fs.defaultFS", "file:///") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "60s")
            
            spark = builder.getOrCreate()
        
        # Set log level to reduce verbose output
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark session created successfully.")
        print(f"🔍 Spark Master: {spark.sparkContext.master}")
        print(f"🔍 Spark Version: {spark.version}")
        print(f"🔍 Distributed mode: {is_distributed}")
        
        return spark
        
    except Exception as e:
        print(f"❗ Failed to create Spark session: {e}")
        import traceback
        traceback.print_exc()
        raise e
        
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: save.py <input_path> <table_name>")
        print(f"Received arguments: {sys.argv}")
        sys.exit(1)
    
    input_path = sys.argv[1]
    table_name = sys.argv[2]
    
    print(f"🚀 Starting data save to MySQL...")
    print(f"📍 Input path: {input_path}")
    print(f"📍 Table name: {table_name}")
    
    # Check environment before proceeding
    check_environment()
    
    spark = None
    try:
        # Create Spark session with fallback mechanism
        spark = create_spark_session()
        
        print(f"� Reading data from: {input_path}")
        print(f"🔍 Input path exists: {os.path.exists(input_path)}")
        
        if not os.path.exists(input_path):
            print(f"❌ Input path does not exist: {input_path}")
            sys.exit(1)
        
        data = spark.read.parquet(input_path)
        
        print(f"📊 Number of records: {data.count()}")
        print("📋 Schema:")
        data.printSchema()
        
        print("📊 Sample data (first 5 rows):")
        data.show(5, truncate=False)
        
        print(f"💾 Saving data to MySQL table: {table_name}")
        save_to_mysql(data, table_name)
        
        print("✅ Data saved to MySQL successfully.")
        
    except Exception as e:
        print(f"❌ Failed to process data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()
            print("✅ Spark session stopped.")
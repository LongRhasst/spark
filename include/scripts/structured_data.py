from pyspark.sql import SparkSession
import sys
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

def structed_data(input_path, output_path):
    spark = None
    try:
        # Check if we're in a Docker/distributed environment
        is_distributed = os.environ.get('YARN_CONF_DIR') or os.environ.get('HADOOP_CONF_DIR')
        
        if is_distributed:
            print("🔧 Attempting Spark configuration for distributed environment...")
            try:
                # Try YARN first
                builder = SparkSession.builder \
                    .appName("StructuredData") \
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
                .appName("StructuredData") \
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
        
    except Exception as e:
        print(f"❗ Failed to connect to Spark: {e}")
        import traceback
        traceback.print_exc()
        return

    try:
        print(f"📥 Reading data from: {input_path}")
        print(f"🔍 Input path exists: {os.path.exists(input_path)}")
        
        if not os.path.exists(input_path):
            print(f"❌ Input file does not exist: {input_path}")
            return
        
        data = spark.read.option('multiline', 'true').json(input_path)
        
        print("✅ Successfully read input file.")
        print(f"📊 Number of records: {data.count()}")
        print(f"� Schema: {data.printSchema()}")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        
        print(f"�💾 Writing data to Parquet at: {output_path}")
        data.write.mode('overwrite').parquet(output_path)
        print("✅ Data written successfully.")

    except Exception as e:
        print(f"❗ Failed to process data: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if spark:
            spark.stop()
            print("✅ Spark session stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_data.py <input_path> <output_path>")
        print(f"Received arguments: {sys.argv}")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    print(f"🚀 Starting structured data processing...")
    print(f"📍 Input path: {input_path}")
    print(f"📍 Output path: {output_path}")
    
    # Check environment before proceeding
    check_environment()
    
    structed_data(input_path, output_path)

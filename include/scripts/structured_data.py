from pyspark.sql import SparkSession
import sys
import os
import time

def structure_data(input_path, output_path):
    spark = None
    try:
        print("🚀 Starting structured data processing...")
        print(f"📍 Input path: {input_path}")
        print(f"📍 Output path: {output_path}")
        
        # Check environment
        print("🔍 Checking environment...")
        print(f"✅ JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
        print(f"✅ SPARK_HOME: {os.environ.get('SPARK_HOME', 'Not set')}")
        print(f"🐍 Python executable: {sys.executable}")
        print(f"📁 Current working directory: {os.getcwd()}")
        
        # Check if input file exists
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Initialize Spark session with proper configuration
        print("🔧 Attempting Spark configuration for distributed environment...")
        spark = SparkSession.builder \
            .appName("StructuredData") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.network.timeout", "300s") \
            .config("spark.rpc.askTimeout", "300s") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.executor.cores", "1") \
            .getOrCreate()
        
        print("✅ Spark session initialized successfully!")
        
        # Load raw data from JSON file
        print("📖 Loading raw data from JSON...")
        df = spark.read.json(input_path)
        
        print(f"📊 Loaded {df.count()} records")
        print("📋 Schema:")
        df.printSchema()
        
        # Perform data transformations
        print("🔄 Performing data transformations...")
        structured_df = df.select(
            "name.common",
            "name.official", 
            "capital",
            "region",
            "subregion",
            "population",
            "area"
        )
        
        print(f"📊 Structured data contains {structured_df.count()} records")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        
        # Write structured data to Parquet format
        print("💾 Writing structured data to Parquet format...")
        structured_df.write.mode('overwrite').parquet(output_path)
        
        print("✅ Structured data written successfully!")
        print(f"📍 Output location: {output_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in structured data processing: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Stop the Spark session
        if spark:
            try:
                spark.stop()
                print("✅ Spark session stopped successfully")
                # Give some time for cleanup
                time.sleep(2)
            except Exception as e:
                print(f"⚠️ Warning: Error stopping Spark session: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: structured_data.py <input_path> <output_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    success = structure_data(input_path, output_path)
    if success:
        print("✅ Structured data processing completed successfully!")
        sys.exit(0)
    else:
        print("❌ Structured data processing failed!")
        sys.exit(1)
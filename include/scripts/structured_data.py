from pyspark.sql import SparkSession
import sys
import os

def structed_data(input_path, output_path):
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("StructuredData") \
            .getOrCreate()
        print("✅ Spark session created successfully.")
    except Exception as e:
        print(f"❗ Failed to connect to Spark: {e}")
        return

    try:
        if not os.path.exists(input_path):
            print(f"❌ Input file does not exist: {input_path}")
            return
        
        print(f"📥 Reading data from: {input_path}")
        data = spark.read.option('multiline', 'true').json(input_path)

        print("✅ Successfully read input file.")
        print(f"💾 Writing data to Parquet at: {output_path}")
        data.write.mode('overwrite').parquet(output_path)
        print("✅ Data written successfully.")

    except Exception as e:
        print(f"❗ Failed to process data: {e}")

    finally:
        if spark:
            spark.stop()
            print("✅ Spark session stopped.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        output_path = '/usr/local/airflow/data/foundation'
        if len(sys.argv) > 2:
            output_path = sys.argv[2]
        structed_data(input_path, output_path)
    else:
        structed_data('./data/raw/raw.json', './data/foundation')
        print("Usage: structured_data.py <input_path> [output_path]")
        # For local test: structured_data('./raw/raw.json', './foundation')

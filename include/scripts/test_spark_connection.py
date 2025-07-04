from pyspark.sql import SparkSession

def check_connection():
    try:
        #initialize Spark session
        spark = SparkSession.builder \
        .appName("SparkConnectionTest") \
        .master("spark://spark-master:7077")\
        .getOrCreate()
        
        print("✓ Spark session initialized successfully")
        print(f"  - Application Name: {spark.sparkContext.appName}")
        print(f"  - Master: {spark.sparkContext.master}")
        print(f"  - Spark Version: {spark.version}")
        
        #test init sparkContest
        print("✓ Testing SparkContext initialization...")
        sc = spark.sparkContext
        print("✓ SparkContext initialized successfully")
        print(f"  - Application ID: {sc.applicationId}")
        print(f"  - Default Parallelism: {sc.defaultParallelism}")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to initialize Spark session: {e}")
        return False
    finally:
        #stop the Spark session
        spark.stop()
        print("✓ Spark session stopped")

if __name__ == "__main__":
    check_connection()
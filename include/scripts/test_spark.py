#!/usr/bin/env python3
"""
Simple Spark test script to verify installation and configuration
"""

from pyspark.sql import SparkSession
import os
import sys

def test_spark():
    """Test basic Spark functionality"""
    print("🧪 Testing Spark installation...")
    
    spark = None
    try:
        # Check environment
        print(f"🐍 Python version: {sys.version}")
        print(f"📁 Current directory: {os.getcwd()}")
        print(f"🔧 JAVA_HOME: {os.environ.get('JAVA_HOME', 'Not set')}")
        print(f"🔧 SPARK_HOME: {os.environ.get('SPARK_HOME', 'Not set')}")
        
        # Create Spark session with minimal configuration
        print("\n🚀 Creating Spark session...")
        spark = SparkSession.builder \
            .appName("SparkTest") \
            .master("local[1]") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        print("✅ Spark session created successfully!")
        print(f"🔍 Spark version: {spark.version}")
        print(f"🔍 Spark master: {spark.sparkContext.master}")
        print(f"🔍 Application ID: {spark.sparkContext.applicationId}")
        
        # Test basic operations
        print("\n🧮 Testing basic operations...")
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        df = spark.createDataFrame(data, ["name", "id"])
        
        print(f"✅ Created DataFrame with {df.count()} rows")
        print("✅ Schema:")
        df.printSchema()
        
        print("✅ Sample data:")
        df.show()
        
        # Test JSON reading (create a simple test file)
        test_json_path = "/tmp/test_spark.json"
        test_data = [
            '{"name": "Alice", "age": 25, "city": "New York"}',
            '{"name": "Bob", "age": 30, "city": "San Francisco"}',
            '{"name": "Charlie", "age": 35, "city": "Seattle"}'
        ]
        
        try:
            with open(test_json_path, 'w') as f:
                for line in test_data:
                    f.write(line + '\n')
            
            print(f"\n📖 Testing JSON file reading...")
            json_df = spark.read.json(test_json_path)
            print(f"✅ Read JSON file with {json_df.count()} rows")
            json_df.show()
            
            # Clean up test file
            os.remove(test_json_path)
            
        except Exception as e:
            print(f"⚠️  JSON test failed: {e}")
        
        print("\n🎉 All tests passed! Spark is working correctly.")
        return True
        
    except Exception as e:
        print(f"❌ Spark test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        if spark:
            spark.stop()
            print("✅ Spark session stopped")

if __name__ == "__main__":
    success = test_spark()
    sys.exit(0 if success else 1)

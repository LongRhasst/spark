#!/usr/bin/env python3
"""
Diagnostic script to check Spark and YARN connectivity
"""
import subprocess
import sys
import os
from urllib.request import urlopen
from urllib.error import URLError
import socket

def check_java():
    """Check if Java is available"""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java is available")
            print(f"   Version info: {result.stderr.split()[2] if result.stderr else 'Unknown'}")
            return True
        else:
            print("❌ Java is not available")
            return False
    except FileNotFoundError:
        print("❌ Java command not found")
        return False

def check_spark():
    """Check if Spark is available"""
    try:
        spark_home = os.environ.get('SPARK_HOME', '/home/airflow/.local/lib/python3.10/site-packages/pyspark')
        spark_submit = f"{spark_home}/bin/spark-submit"
        
        if os.path.exists(spark_submit):
            print(f"✅ Spark submit found at: {spark_submit}")
            result = subprocess.run([spark_submit, '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                print("✅ Spark submit is working")
                return True
            else:
                print(f"❌ Spark submit failed: {result.stderr}")
                return False
        else:
            print(f"❌ Spark submit not found at: {spark_submit}")
            return False
    except Exception as e:
        print(f"❌ Error checking Spark: {e}")
        return False

def check_yarn_services():
    """Check if YARN services are accessible"""
    services = {
        'ResourceManager': ('resourcemanager', 8088),
        'NameNode': ('namenode', 9870),
        'DataNode': ('datanode', 9864),
        'NodeManager': ('nodemanager-1', 8042)
    }
    
    accessible_services = []
    
    for service_name, (host, port) in services.items():
        try:
            # Try to connect to the service
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f"✅ {service_name} is accessible at {host}:{port}")
                accessible_services.append(service_name)
            else:
                print(f"❌ {service_name} is not accessible at {host}:{port}")
        except Exception as e:
            print(f"❌ Error checking {service_name}: {e}")
    
    return accessible_services

def check_yarn_api():
    """Check YARN ResourceManager API"""
    try:
        url = 'http://resourcemanager:8088/ws/v1/cluster/info'
        response = urlopen(url, timeout=10)
        if response.status == 200:
            print("✅ YARN ResourceManager API is accessible")
            return True
        else:
            print(f"❌ YARN ResourceManager API returned status: {response.status}")
            return False
    except URLError as e:
        print(f"❌ Cannot access YARN ResourceManager API: {e}")
        return False
    except Exception as e:
        print(f"❌ Error checking YARN API: {e}")
        return False

def check_hdfs():
    """Check HDFS connectivity"""
    try:
        # Try to list HDFS root directory
        result = subprocess.run(['hdfs', 'dfs', '-ls', '/'], capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ HDFS is accessible")
            print("   HDFS root directory contents:")
            for line in result.stdout.split('\n')[:5]:  # Show first 5 lines
                if line.strip():
                    print(f"   {line}")
            return True
        else:
            print(f"❌ HDFS is not accessible: {result.stderr}")
            return False
    except FileNotFoundError:
        print("❌ HDFS command not found")
        return False
    except subprocess.TimeoutExpired:
        print("❌ HDFS command timed out")
        return False
    except Exception as e:
        print(f"❌ Error checking HDFS: {e}")
        return False

def test_spark_local():
    """Test Spark in local mode"""
    print("\n🧪 Testing Spark in local mode...")
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("DiagnosticTest") \
            .master("local[1]") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .getOrCreate()
        
        # Create a simple DataFrame
        data = [("test", 1), ("data", 2)]
        df = spark.createDataFrame(data, ["col1", "col2"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ Spark local mode test successful. DataFrame count: {count}")
        return True
    except Exception as e:
        print(f"❌ Spark local mode test failed: {e}")
        return False

def test_spark_yarn():
    """Test Spark in YARN mode"""
    print("\n🧪 Testing Spark in YARN mode...")
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("DiagnosticTestYARN") \
            .master("yarn") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.yarn.appMasterEnv.YARN_CONF_DIR", "/opt/hadoop/etc/hadoop") \
            .config("spark.yarn.appMasterEnv.HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop") \
            .config("spark.executorEnv.YARN_CONF_DIR", "/opt/hadoop/etc/hadoop") \
            .config("spark.executorEnv.HADOOP_CONF_DIR", "/opt/hadoop/etc/hadoop") \
            .getOrCreate()
        
        # Create a simple DataFrame
        data = [("test", 1), ("yarn", 2)]
        df = spark.createDataFrame(data, ["col1", "col2"])
        count = df.count()
        
        spark.stop()
        
        print(f"✅ Spark YARN mode test successful. DataFrame count: {count}")
        return True
    except Exception as e:
        print(f"❌ Spark YARN mode test failed: {e}")
        return False

def main():
    print("🔍 Spark and YARN Connectivity Diagnostic")
    print("=" * 50)
    
    print("\n1. Checking Java...")
    java_ok = check_java()
    
    print("\n2. Checking Spark...")
    spark_ok = check_spark()
    
    print("\n3. Checking YARN services...")
    accessible_services = check_yarn_services()
    
    print("\n4. Checking YARN API...")
    yarn_api_ok = check_yarn_api()
    
    print("\n5. Checking HDFS...")
    hdfs_ok = check_hdfs()
    
    print("\n6. Testing Spark modes...")
    local_test_ok = test_spark_local()
    yarn_test_ok = test_spark_yarn() if len(accessible_services) >= 2 else False
    
    print("\n" + "=" * 50)
    print("📊 DIAGNOSIS SUMMARY")
    print("=" * 50)
    
    if java_ok and spark_ok and local_test_ok:
        print("✅ Spark local mode is working correctly")
        print("   Recommendation: Use local mode for development")
    else:
        print("❌ Spark local mode has issues")
    
    if len(accessible_services) >= 2 and yarn_api_ok and yarn_test_ok:
        print("✅ YARN cluster is working correctly")
        print("   Recommendation: You can use YARN mode")
    else:
        print("❌ YARN cluster has issues")
        print("   Issues found:")
        if len(accessible_services) < 2:
            print(f"   - Only {len(accessible_services)} YARN services are accessible")
        if not yarn_api_ok:
            print("   - YARN ResourceManager API is not accessible")
        if not yarn_test_ok:
            print("   - Spark YARN test failed")
        print("   Recommendation: Use local mode or fix YARN cluster")
    
    print("\n🎯 RECOMMENDED ACTIONS:")
    if local_test_ok and not yarn_test_ok:
        print("1. Use the local fallback DAG: 'spark_processing_pipeline_local'")
        print("2. Or use the smart fallback DAG: 'spark_processing_pipeline_smart'")
    elif yarn_test_ok:
        print("1. Your YARN cluster is working, the original DAG should work")
        print("2. Check Airflow logs for more specific error details")
    else:
        print("1. Fix Spark installation issues first")
        print("2. Check Java installation and SPARK_HOME environment variable")

if __name__ == "__main__":
    main()

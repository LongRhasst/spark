#!/usr/bin/env python3
"""
Test script for all Spark scripts to verify they work with fallback mechanism
"""

import os
import sys
import subprocess
import tempfile
import json

def test_spark_script(script_name, script_path, test_args=None):
    """Test a Spark script with fallback mechanism"""
    print(f"\n🧪 Testing {script_name}...")
    print(f"📁 Script path: {script_path}")
    
    if not os.path.exists(script_path):
        print(f"❌ Script not found: {script_path}")
        return False
    
    try:
        # Prepare test command
        cmd = ['python', script_path]
        if test_args:
            cmd.extend(test_args)
        
        print(f"🔧 Running command: {' '.join(cmd)}")
        
        # Run the script
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        print(f"📊 Return code: {result.returncode}")
        print(f"📝 Output: {result.stdout}")
        
        if result.stderr:
            print(f"⚠️  Stderr: {result.stderr}")
        
        if result.returncode == 0:
            print(f"✅ {script_name} test passed")
            return True
        else:
            print(f"❌ {script_name} test failed")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏰ {script_name} test timed out")
        return False
    except Exception as e:
        print(f"❌ {script_name} test error: {e}")
        return False

def create_test_data():
    """Create test data files for testing"""
    print("🔧 Creating test data...")
    
    # Create test directories
    test_dir = "/tmp/spark_test"
    os.makedirs(test_dir, exist_ok=True)
    
    # Create test JSON data
    test_json_data = [
        {
            "name": {"common": "Vietnam", "official": "Socialist Republic of Vietnam"},
            "ccn3": "704",
            "cca3": "VNM",
            "latlng": [16.0, 108.0],
            "independent": True,
            "status": "officially-assigned",
            "translations": {"eng": {"official": "Vietnam", "common": "Vietnam"}},
            "capital": ["Hanoi"],
            "altSpellings": ["VN", "Vietnam", "Viet Nam"],
            "languages": {"eng": "English", "vie": "Vietnamese"},
            "region": "Asia",
            "area": 331212.0,
            "maps": {"googleMaps": "https://goo.gl/maps/Vietnam"},
            "timezones": ["UTC+07:00"],
            "continents": ["Asia"],
            "flags": {"svg": "https://flagcdn.com/vn.svg"},
            "startOfWeek": "monday"
        }
    ]
    
    # Write test JSON file
    test_json_path = os.path.join(test_dir, "test_data.json")
    with open(test_json_path, 'w') as f:
        for item in test_json_data:
            json.dump(item, f)
            f.write('\n')
    
    print(f"✅ Test data created at: {test_json_path}")
    return test_dir, test_json_path

def main():
    """Main test function"""
    print("🚀 Starting comprehensive Spark scripts test...")
    
    # Create test data
    test_dir, test_json_path = create_test_data()
    
    # Define test paths
    foundation_path = os.path.join(test_dir, "foundation")
    trusted_path = os.path.join(test_dir, "trusted")
    
    # Test scripts
    scripts_base = "/opt/airflow/include/scripts"
    tests = [
        {
            "name": "test_spark.py",
            "path": os.path.join(scripts_base, "test_spark.py"),
            "args": None
        },
        {
            "name": "structured_data.py",
            "path": os.path.join(scripts_base, "structured_data.py"),
            "args": [test_json_path, foundation_path]
        },
        {
            "name": "transform_data.py",
            "path": os.path.join(scripts_base, "transform_data.py"),
            "args": [foundation_path, trusted_path]
        }
        # Note: save.py test would require MySQL connection, skipping for now
    ]
    
    results = []
    
    for test in tests:
        success = test_spark_script(test["name"], test["path"], test["args"])
        results.append((test["name"], success))
    
    # Summary
    print("\n📊 Test Results Summary:")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{name:25} {status}")
        if success:
            passed += 1
    
    print("=" * 50)
    print(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Spark scripts are working correctly.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

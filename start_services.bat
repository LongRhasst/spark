@echo off
REM Spark Startup Script for Windows
REM This script ensures proper startup order and handles SparkContext initialization errors

echo 🚀 Starting Spark Data Pipeline Services...

echo 📋 Step 1: Starting base services...
docker-compose up -d mysql

echo 📋 Step 2: Waiting for MySQL to be ready...
timeout /t 30 /nobreak

echo 📋 Step 3: Starting Hadoop services...
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d namenode datanode

echo 📋 Step 4: Waiting for Hadoop services...
timeout /t 45 /nobreak

echo 📋 Step 5: Starting YARN services...
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d resourcemanager nodemanager-1

echo 📋 Step 6: Starting Spark services...
docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d spark-master spark-worker-1

echo 📋 Step 7: Waiting for Spark services...
timeout /t 30 /nobreak

echo 📋 Step 8: Starting Airflow services...
docker-compose up -d airflow-init

echo 📋 Step 9: Waiting for Airflow initialization...
timeout /t 60 /nobreak

echo 📋 Step 10: Starting Airflow webserver and scheduler...
docker-compose up -d webserver scheduler

echo 📋 Step 11: Final setup wait...
timeout /t 30 /nobreak

echo 🎉 All services started!
echo.
echo 🌐 Access points:
echo    - Airflow Web UI: http://localhost:8080 (admin/admin)
echo    - Spark Master UI: http://localhost:8081
echo    - Hadoop Namenode UI: http://localhost:9870
echo.
echo 📚 Next steps:
echo    1. Test Spark: docker-compose exec webserver python /opt/airflow/include/scripts/test_spark.py
echo    2. Use local DAG: 'spark_processing_pipeline_local' for guaranteed operation
echo    3. Check logs: docker-compose logs -f [service_name]
echo.
echo 🛠️  If SparkContext errors occur:
echo    - Run: restart_services.bat
echo    - Use local mode DAG: 'spark_processing_pipeline_local'

pause

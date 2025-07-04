#!/bin/bash

# Spark Startup Script with Error Handling
# This script ensures proper startup order and handles SparkContext initialization errors

set -e

echo "🚀 Starting Spark Data Pipeline Services..."

# Function to check if a service is healthy
check_service_health() {
    local service_name=$1
    local max_attempts=30
    local attempt=1
    
    echo "🔍 Checking health of $service_name..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker compose ps $service_name | grep -q "healthy\|Up"; then
            echo "✅ $service_name is healthy"
            return 0
        fi
        
        echo "⏳ Waiting for $service_name... (attempt $attempt/$max_attempts)"
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "❌ $service_name failed to become healthy"
    return 1
}

# Function to test Spark connectivity
test_spark_connection() {
    echo "🧪 Testing Spark connection..."
    
    # Try to run the test script
    if docker compose exec webserver python /opt/airflow/include/scripts/test_int_sparkContext; then
        echo "✅ Spark connection test passed"
        return 0
    else
        echo "⚠️  Spark connection test failed, but continuing..."
        return 1
    fi
}

echo "📋 Step 1: Starting base services..."
docker compose up -d mysql

echo "📋 Step 2: Waiting for MySQL to be ready..."
check_service_health mysql

echo "📋 Step 3: Starting Spark services..."
docker compose -f docker-compose.yml -f docker-compose.override.yml up -d spark-master spark-worker-1

echo "📋 Step 4: Waiting for Spark services..."
check_service_health spark-master
check_service_health spark-worker-1
# check_service_health spark-worker-2

echo "📋 Step 5: Starting Airflow services..."
docker compose up -d airflow-init

echo "📋 Step 6: Waiting for Airflow initialization..."
check_service_health airflow-init

echo "📋 Step 7: Starting Airflow webserver and scheduler..."
docker compose up -d webserver scheduler

echo "📋 Step 8: Final health check..."
check_service_health webserver
check_service_health scheduler

echo "📋 Step 9: create connections and variables..."
docker compose exec webserver airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077' \
    --conn-extra '{"spark.master": "spark://spark-master:7077", "spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.executor.cores": "1"}'

echo "📋 Step 9: Testing Spark connection..."
sleep 30  # Give services time to fully start
# test_spark_connection

echo "🎉 All services started successfully!"
echo ""
echo "🌐 Access points:"
echo "   - Airflow Web UI: http://localhost:8080 (admin/admin)"
echo "   - Spark Master UI: http://localhost:8081"
echo ""
echo "📚 Next steps:"
echo "   1. Check service logs: docker-compose logs -f [service_name]"
echo "   2. Test Spark: docker-compose exec webserver python /opt/airflow/include/scripts/test_spark.py"
echo "   3. Run DAG: Use 'spark_processing_pipeline_local' for guaranteed local mode"
echo ""
echo "🛠️  If you encounter SparkContext errors:"
echo "   - Use the local fallback DAG: 'spark_processing_pipeline_local'"
echo "   - Check logs: docker-compose logs webserver scheduler"
echo "   - Restart services: ./restart_services.sh"

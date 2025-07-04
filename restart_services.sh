#!/bin/bash

# Restart script for when SparkContext errors occur

echo "🔄 Restarting Spark Data Pipeline Services..."

echo "📋 Step 1: Stopping all services..."
docker compose -f docker-compose.yml -f docker-compose.override.yml down

echo "📋 Step 2: Cleaning up containers and networks..."
docker system prune -f

echo "📋 Step 3: Restarting services in proper order..."
./start_services.sh

echo "✅ Restart complete!"

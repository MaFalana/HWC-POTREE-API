#!/bin/bash

# Test ortho conversion locally
# Usage: ./test_ortho_local.sh <path_to_geotiff>

set -e

ORTHO_FILE=$1

if [ -z "$ORTHO_FILE" ]; then
    echo "Usage: ./test_ortho_local.sh <path_to_geotiff>"
    exit 1
fi

if [ ! -f "$ORTHO_FILE" ]; then
    echo "Error: File not found: $ORTHO_FILE"
    exit 1
fi

echo "🚀 Starting Docker services..."
docker-compose up -d --build

echo "⏳ Waiting for services to be ready..."
sleep 10

# Check if service is up
until curl -s http://localhost:8000/health > /dev/null; do
    echo "Waiting for API to be ready..."
    sleep 2
done

echo "✅ API is ready!"

# Create test project
echo "📝 Creating test project..."
PROJECT_ID="2021-274-S"

curl -X POST http://localhost:8000/projects/ \
  -H "Content-Type: application/json" \
  -d '{
    "_id": "'"$PROJECT_ID"'",
    "name": "Icon on Main",
    "client": "Edward Rose Properties",
    "date": "2025-11-26T00:00:00",
    "crs": {
      "_id": "6459",
      "name": "NAD83(2011) / Indiana East (ftUS)",
      "proj4": "+proj=tmerc +lat_0=37.5 +lon_0=-85.6666666666667 +k=0.999966667 +x_0=99999.9998983997 +y_0=249999.9998984 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=us-ft +no_defs +type=crs"
    }
  }' 2>/dev/null || echo "Project may already exist"

echo ""
echo "📤 Uploading ortho file: $ORTHO_FILE"

RESPONSE=$(curl -X POST http://localhost:8000/projects/$PROJECT_ID/ortho \
  -F "file=@$ORTHO_FILE" \
  -s)

echo "Response: $RESPONSE"

JOB_ID=$(echo $RESPONSE | grep -o '"job_id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$JOB_ID" ]; then
    echo "❌ Failed to get job ID"
    exit 1
fi

echo "✅ Job created: $JOB_ID"
echo ""
echo "📊 Monitoring job status..."
echo "Press Ctrl+C to stop monitoring"
echo ""

# Monitor job status
while true; do
    STATUS=$(curl -s http://localhost:8000/jobs/$JOB_ID | python3 -c "import sys, json; data=json.load(sys.stdin); print(f\"{data['status']}: {data.get('progress_message', 'N/A')}\")" 2>/dev/null || echo "error")
    
    echo "[$(date +%H:%M:%S)] $STATUS"
    
    if [[ "$STATUS" == *"completed"* ]]; then
        echo ""
        echo "✅ Job completed successfully!"
        echo ""
        echo "📋 Project details:"
        curl -s http://localhost:8000/projects/$PROJECT_ID | python3 -m json.tool
        break
    elif [[ "$STATUS" == *"failed"* ]]; then
        echo ""
        echo "❌ Job failed!"
        echo ""
        echo "📋 Job details:"
        curl -s http://localhost:8000/jobs/$JOB_ID | python3 -m json.tool
        break
    fi
    
    sleep 2
done

echo ""
echo "📜 Checking logs..."
docker-compose logs --tail=50 web

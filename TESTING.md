# 🧪 QupidLineage Backend Testing Guide

## 📋 Quick Start Testing

### Prerequisites
- Docker and Docker Compose installed
- Git installed
- `curl` or API testing tool (Postman, Insomnia)

### 1. Clone and Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/qupid-lineage-backend.git
cd qupid-lineage-backend

# Copy environment template
cp env.template .env

# Edit .env file with your API keys
nano .env
```

### 2. Start Services

```bash
# Start all services (database + backend)
docker-compose up -d

# Check if services are running
docker-compose ps

# View logs
docker-compose logs -f backend
```

### 3. Basic Health Check

```bash
# Wait 30-60 seconds for services to start, then test
curl http://localhost:8080/api/extraction/health

# Expected response:
# {"status":"UP","timestamp":"2024-01-15T10:30:00","version":"1.0.0"}
```

## 🔧 API Testing

### Health and Status Endpoints

```bash
# Health check
curl http://localhost:8080/api/extraction/health

# Actuator health (detailed)
curl http://localhost:8080/api/actuator/health
```

### Repository Scanning Endpoints

#### 1. Start a Repository Scan

```bash
curl -X POST http://localhost:8080/api/extraction/scan \
  -H "Content-Type: application/json" \
  -d '{
    "repositoryUrl": "https://github.com/dbt-labs/jaffle_shop",
    "branch": "main",
    "triggeredBy": "test-user"
  }'
```

**Expected Response:**
```json
{
  "runId": "123e4567-e89b-12d3-a456-426614174000",
  "status": "STARTED",
  "message": "Extraction started successfully"
}
```

#### 2. Get All Extraction Runs

```bash
curl http://localhost:8080/api/extraction/runs
```

**Expected Response:**
```json
{
  "content": [
    {
      "runId": "123e4567-e89b-12d3-a456-426614174000",
      "repositoryUrl": "https://github.com/dbt-labs/jaffle_shop",
      "branch": "main",
      "phase": "STARTED",
      "startedAt": "2024-01-15T10:30:00",
      "triggeredBy": "test-user"
    }
  ],
  "totalElements": 1,
  "totalPages": 1
}
```

#### 3. Get Specific Run Details

```bash
# Replace {runId} with actual run ID from previous response
curl http://localhost:8080/api/extraction/run/{runId}
```

#### 4. Cancel an Extraction Run

```bash
curl -X POST http://localhost:8080/api/extraction/run/{runId}/cancel
```

### Webhook Testing

#### Send Test Webhook Event

```bash
curl -X POST http://localhost:8080/api/extraction/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "file_extraction",
    "runId": "123e4567-e89b-12d3-a456-426614174000",
    "podId": "test-pod-1",
    "timestamp": "2024-01-15T10:30:00",
    "data": {
      "filePath": "/path/to/test.sql",
      "status": "SUCCESS",
      "tables": [
        {
          "name": "customers",
          "role": "SOURCE"
        }
      ]
    }
  }'
```

### Lineage Query Endpoints

```bash
# Get lineage data for a specific run
curl http://localhost:8080/api/lineage/run/{runId}

# Get table details
curl http://localhost:8080/api/lineage/table/{tableName}

# Get lineage edges
curl http://localhost:8080/api/lineage/edges?runId={runId}
```

## 📊 Database Testing

### Connect to Database

```bash
# Using Docker exec
docker exec -it lineage-postgres psql -U postgres -d lineage_extractor

# Or using local psql client
psql -h localhost -U postgres -d lineage_extractor
```

### Sample Queries

```sql
-- Check extraction runs
SELECT run_id, repository_url, phase, started_at 
FROM extraction_runs 
ORDER BY created_at DESC 
LIMIT 5;

-- Check files processed
SELECT f.file_path, f.status, f.file_type, er.repository_url
FROM files f
JOIN extraction_runs er ON f.run_id = er.run_id
ORDER BY f.created_at DESC 
LIMIT 10;

-- Check tables discovered
SELECT t.table_name, t.table_role, f.file_path
FROM tables t
JOIN files f ON t.file_id = f.file_id
ORDER BY t.created_at DESC 
LIMIT 10;

-- Check lineage edges
SELECT from_table, to_table, transformation_type
FROM lineage_edges
ORDER BY created_at DESC 
LIMIT 10;

-- Get extraction summary
SELECT * FROM extraction_run_summary;
```

## 🔍 Advanced Testing Scenarios

### Test Case 1: Complete Workflow

```bash
# 1. Start extraction
RESPONSE=$(curl -s -X POST http://localhost:8080/api/extraction/scan \
  -H "Content-Type: application/json" \
  -d '{
    "repositoryUrl": "https://github.com/dbt-labs/jaffle_shop",
    "branch": "main",
    "triggeredBy": "integration-test"
  }')

# 2. Extract run ID
RUN_ID=$(echo $RESPONSE | grep -o '"runId":"[^"]*' | cut -d'"' -f4)
echo "Run ID: $RUN_ID"

# 3. Check status
curl http://localhost:8080/api/extraction/run/$RUN_ID

# 4. Send webhook events to simulate processing
curl -X POST http://localhost:8080/api/extraction/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "extraction_run_started",
    "runId": "'$RUN_ID'",
    "podId": "test-pod",
    "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S)'",
    "data": {
      "totalFiles": 5
    }
  }'

# 5. Send file processing events
curl -X POST http://localhost:8080/api/extraction/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "file_extraction",
    "runId": "'$RUN_ID'",
    "podId": "test-pod",
    "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S)'",
    "data": {
      "filePath": "models/customers.sql",
      "status": "SUCCESS",
      "fileHash": "abc123",
      "tables": [
        {
          "name": "customers",
          "role": "TARGET",
          "columns": ["customer_id", "first_name", "last_name"]
        }
      ],
      "lineageEdges": [
        {
          "fromTable": "raw_customers",
          "toTable": "customers",
          "transformationType": "SELECT"
        }
      ]
    }
  }'

# 6. Complete the run
curl -X POST http://localhost:8080/api/extraction/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "extraction_run_completed",
    "runId": "'$RUN_ID'",
    "podId": "test-pod",
    "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S)'",
    "data": {
      "totalFiles": 5,
      "processedFiles": 5,
      "succeededFiles": 5,
      "failedFiles": 0
    }
  }'

# 7. Verify final status
curl http://localhost:8080/api/extraction/run/$RUN_ID
```

### Test Case 2: Error Handling

```bash
# Test invalid repository URL
curl -X POST http://localhost:8080/api/extraction/scan \
  -H "Content-Type: application/json" \
  -d '{
    "repositoryUrl": "invalid-url",
    "branch": "main",
    "triggeredBy": "error-test"
  }'

# Test missing required fields
curl -X POST http://localhost:8080/api/extraction/scan \
  -H "Content-Type: application/json" \
  -d '{
    "repositoryUrl": "https://github.com/test/repo"
  }'

# Test non-existent run ID
curl http://localhost:8080/api/extraction/run/00000000-0000-0000-0000-000000000000
```

### Test Case 3: Performance Testing

```bash
# Create multiple concurrent extractions
for i in {1..5}; do
  curl -X POST http://localhost:8080/api/extraction/scan \
    -H "Content-Type: application/json" \
    -d '{
      "repositoryUrl": "https://github.com/test/repo-'$i'",
      "branch": "main",
      "triggeredBy": "performance-test-'$i'"
    }' &
done
wait

# Check all runs
curl http://localhost:8080/api/extraction/runs?size=20
```

## 📝 Testing with Different Tools

### Using Postman

1. **Import Collection**: Create a Postman collection with the following requests:
   - GET Health Check: `http://localhost:8080/api/extraction/health`
   - POST Start Scan: `http://localhost:8080/api/extraction/scan`
   - GET List Runs: `http://localhost:8080/api/extraction/runs`
   - GET Run Details: `http://localhost:8080/api/extraction/run/{{runId}}`
   - POST Webhook: `http://localhost:8080/api/extraction/webhook`

2. **Environment Variables**: Set up Postman environment with:
   - `baseUrl`: `http://localhost:8080/api`
   - `runId`: (to be set dynamically)

### Using HTTPie

```bash
# Install HTTPie
pip install httpie

# Health check
http GET localhost:8080/api/extraction/health

# Start scan
http POST localhost:8080/api/extraction/scan \
  repositoryUrl="https://github.com/dbt-labs/jaffle_shop" \
  branch="main" \
  triggeredBy="httpie-test"

# Get runs
http GET localhost:8080/api/extraction/runs
```

### Using Python Requests

```python
import requests
import json
from datetime import datetime

BASE_URL = "http://localhost:8080/api"

# Health check
response = requests.get(f"{BASE_URL}/extraction/health")
print(f"Health: {response.json()}")

# Start extraction
scan_data = {
    "repositoryUrl": "https://github.com/dbt-labs/jaffle_shop",
    "branch": "main",
    "triggeredBy": "python-test"
}

response = requests.post(f"{BASE_URL}/extraction/scan", json=scan_data)
run_data = response.json()
run_id = run_data["runId"]
print(f"Started run: {run_id}")

# Check status
response = requests.get(f"{BASE_URL}/extraction/run/{run_id}")
print(f"Run status: {response.json()}")

# Send webhook
webhook_data = {
    "eventType": "file_extraction",
    "runId": run_id,
    "podId": "python-test-pod",
    "timestamp": datetime.utcnow().isoformat(),
    "data": {
        "filePath": "test.sql",
        "status": "SUCCESS"
    }
}

response = requests.post(f"{BASE_URL}/extraction/webhook", json=webhook_data)
print(f"Webhook response: {response.status_code}")
```

## 🔍 Troubleshooting

### Common Issues

1. **Service not starting**:
   ```bash
   # Check logs
   docker-compose logs backend
   docker-compose logs postgres
   
   # Restart services
   docker-compose down
   docker-compose up -d
   ```

2. **Database connection issues**:
   ```bash
   # Test database connectivity
   docker exec lineage-postgres pg_isready -U postgres
   
   # Check database logs
   docker logs lineage-postgres
   ```

3. **API returning 500 errors**:
   ```bash
   # Check application logs
   docker-compose logs backend | grep ERROR
   
   # Check database schema
   docker exec -it lineage-postgres psql -U postgres -d lineage_extractor -c "\dt"
   ```

4. **Port conflicts**:
   ```bash
   # Check what's using port 8080
   lsof -i :8080
   
   # Kill process if needed
   kill -9 $(lsof -ti:8080)
   ```

### Health Monitoring

```bash
# Monitor service health
watch -n 5 'curl -s http://localhost:8080/api/extraction/health | jq'

# Monitor database connections
docker exec lineage-postgres psql -U postgres -d lineage_extractor -c "SELECT count(*) FROM pg_stat_activity;"

# Monitor disk usage
docker system df
```

### Performance Monitoring

```bash
# Check memory usage
docker stats lineage-backend lineage-postgres

# Check application metrics
curl http://localhost:8080/api/actuator/metrics

# Check JVM info
curl http://localhost:8080/api/actuator/info
```

## 📊 Expected Test Results

### Successful Health Check
```json
{
  "status": "UP",
  "timestamp": "2024-01-15T10:30:00",
  "version": "1.0.0",
  "database": "UP",
  "diskSpace": "UP"
}
```

### Successful Scan Start
```json
{
  "runId": "123e4567-e89b-12d3-a456-426614174000",
  "status": "STARTED",
  "message": "Extraction started successfully",
  "repositoryUrl": "https://github.com/dbt-labs/jaffle_shop",
  "branch": "main"
}
```

### Successful Webhook Processing
```json
{
  "status": "SUCCESS",
  "message": "Webhook processed successfully",
  "eventType": "file_extraction",
  "runId": "123e4567-e89b-12d3-a456-426614174000"
}
```

## 🎯 Test Checklist

- [ ] Services start successfully with `docker-compose up -d`
- [ ] Health endpoint returns 200 OK
- [ ] Database schema is created correctly
- [ ] Can start repository scan
- [ ] Can retrieve extraction runs list
- [ ] Can get specific run details
- [ ] Can process webhook events
- [ ] Can cancel extraction runs
- [ ] Error handling works for invalid inputs
- [ ] Database queries return expected data
- [ ] Services restart successfully after failure
- [ ] Performance is acceptable under load

## 📞 Support

If tests fail:
1. Check the troubleshooting section above
2. Review Docker logs: `docker-compose logs`
3. Verify environment variables in `.env` file
4. Ensure all prerequisites are installed
5. Check GitHub repository for latest updates

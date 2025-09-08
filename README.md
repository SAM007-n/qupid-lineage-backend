# 🚀 QupidLineage Backend API

Spring Boot backend service for SQL dependency extraction with database persistence, webhook integration, and comprehensive lineage tracking.

[![Java](https://img.shields.io/badge/Java-17+-orange.svg)](https://openjdk.java.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.0-green.svg)](https://spring.io/projects/spring-boot)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)](https://www.docker.com/)

## 🏗️ **Architecture**

This backend implements the **Approach 2** architecture from our design:

- **REST API**: Handles repository scan requests
- **Database**: PostgreSQL with JPA entities
- **Webhook Handler**: Receives events from extraction pods
- **Job Management**: Tracks extraction progress and status

## 📋 **Features**

- ✅ **Repository Scan Management**: Start, track, and cancel extraction runs
- ✅ **Database Persistence**: Store extraction runs, files, tables, and lineage edges
- ✅ **Webhook Integration**: Receive real-time updates from extraction pods
- ✅ **Progress Tracking**: Monitor extraction progress with detailed statistics
- ✅ **RESTful API**: Clean REST endpoints for all operations

## 🚀 **Quick Start for Testing**

### **Option 1: Docker Compose (Recommended)**

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/qupid-lineage-backend.git
cd qupid-lineage-backend

# Copy environment template
cp env.template .env
# Edit .env with your API keys

# Start all services
docker-compose up -d

# Test the API
curl http://localhost:8080/api/extraction/health
```

### **Option 2: Manual Setup**

#### **Prerequisites**

- Java 17+
- Maven 3.6+
- PostgreSQL 15+
- Docker (optional)

### **1. Start PostgreSQL Database**

```bash
# Start PostgreSQL with Docker
docker run --name lineage-postgres \
  -e POSTGRES_PASSWORD=lineage123 \
  -e POSTGRES_DB=lineage_extractor \
  -p 5432:5432 \
  -d postgres:15
```

### **2. Run Database Schema**

Execute the SQL script in your PostgreSQL database:
```bash
# Run the schema creation script
psql -h localhost -U postgres -d lineage_extractor -f schema.sql
```

### **3. Configure Environment**

Create `.env` file or set environment variables:
```bash
export GROQ_API_KEY="your-groq-api-key"
export GITHUB_TOKEN="your-github-token"
```

### **4. Build and Run**

```bash
# Build the project
mvn clean install

# Run the application
mvn spring-boot:run
```

The application will start on `http://localhost:8080`

## 📊 **Database Schema**

The backend uses these main entities:

- **`ExtractionRun`**: Tracks each repository scan session
- **`File`**: Stores information about each SQL/Jinja2 file processed
- **`Table`**: Contains tables found within each file
- **`LineageEdge`**: Stores relationships between tables and columns
- **`JobStatus`**: Tracks real-time progress of extraction jobs

## 🔌 **API Endpoints**

### **Repository Scanning**

```bash
# Start a new repository scan
POST /api/extraction/scan
{
  "repositoryUrl": "https://github.com/your-username/your-repo",
  "branch": "main",
  "commitHash": "optional-commit-hash",
  "runMode": "FULL",
  "triggeredBy": "api"
}

# Get extraction run status
GET /api/extraction/run/{runId}

# Get all extraction runs
GET /api/extraction/runs?page=0&size=20

# Cancel an extraction run
POST /api/extraction/run/{runId}/cancel
```

### **Webhook Endpoint**

```bash
# Webhook for extraction pods
POST /api/extraction/webhook
{
  "eventType": "file_extraction",
  "runId": "uuid",
  "podId": "pod-123",
  "data": { ... },
  "timestamp": "2024-01-15T10:30:00"
}
```

### **Health Check**

```bash
# Health check
GET /api/extraction/health
```

## 🔄 **Webhook Events**

The backend handles these webhook event types:

- `extraction_run_started`: Run initialization
- `extraction_run_completed`: Run completion
- `extraction_run_failed`: Run failure
- `file_extraction`: File processing results
- `progress_update`: Progress updates
- `job_status_update`: Job status changes

## 🗄️ **Database Operations**

### **Repository Queries**

```java
// Find runs by repository
List<ExtractionRun> runs = extractionRunRepository.findByRepositoryUrlOrderByCreatedAtDesc(repoUrl);

// Get recent runs
List<ExtractionRun> recent = extractionRunRepository.findRecentRuns(since);

// Count by phase
long running = extractionRunRepository.countByPhase(ExtractionPhase.STARTED);
```

### **File Operations**

```java
// Get files for a run
List<File> files = fileRepository.findByExtractionRunRunIdOrderByFilePath(runId);

// Count files by status
long succeeded = fileRepository.countByRunIdAndStatus(runId, FileStatus.SUCCESS);
```

### **Lineage Queries**

```java
// Get lineage edges for a run
List<LineageEdge> edges = lineageEdgeRepository.findByRunId(runId);

// Count edge types
long tableEdges = lineageEdgeRepository.countByEdgeTypeAndRunId(EdgeType.TABLE_EDGE, runId);
```

## 🔧 **Configuration**

### **Application Properties**

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/lineage_extractor
    username: postgres
    password: lineage123

app:
  webhook:
    base-url: http://localhost:8080/api/webhook
  extraction:
    default-timeout: 3600
    max-retries: 3
  groq:
    api-key: ${GROQ_API_KEY}
  github:
    token: ${GITHUB_TOKEN}
```

## 🧪 **Testing**

### **Manual Testing**

```bash
# Test health endpoint
curl http://localhost:8080/api/extraction/health

# Start a scan
curl -X POST http://localhost:8080/api/extraction/scan \
  -H "Content-Type: application/json" \
  -d '{
    "repositoryUrl": "https://github.com/dbt-labs/jaffle_shop",
    "branch": "main",
    "triggeredBy": "test"
  }'

# Get run status (replace with actual run ID)
curl http://localhost:8080/api/extraction/run/{runId}
```

### **Unit Testing**

```bash
# Run tests
mvn test
```

## 🚀 **Next Steps**

1. **Job Queue Integration**: Add Redis/RabbitMQ for job distribution
2. **Enhanced Webhook Handling**: Implement detailed event processing
3. **Authentication**: Add security and user management
4. **Monitoring**: Add metrics and alerting
5. **Frontend Integration**: Connect with React/Angular frontend

## 📝 **Development**

### **Project Structure**

```
src/main/java/com/lineage/
├── SqlDependencyExtractorApplication.java
├── controller/
│   └── ExtractionController.java
├── service/
│   └── ExtractionService.java
├── repository/
│   ├── ExtractionRunRepository.java
│   ├── FileRepository.java
│   ├── TableRepository.java
│   ├── LineageEdgeRepository.java
│   └── JobStatusRepository.java
├── entity/
│   ├── ExtractionRun.java
│   ├── File.java
│   ├── Table.java
│   ├── LineageEdge.java
│   └── JobStatus.java
└── dto/
    ├── RepositoryScanRequest.java
    └── WebhookEvent.java
```

### **Building**

```bash
# Clean build
mvn clean install

# Run with profile
mvn spring-boot:run -Dspring.profiles.active=dev

# Package for deployment
mvn package
```

## 🔍 **Troubleshooting**

### **Common Issues**

1. **Database Connection**: Ensure PostgreSQL is running and accessible
2. **Port Conflicts**: Check if port 8080 is available
3. **Environment Variables**: Verify GROQ_API_KEY and GITHUB_TOKEN are set
4. **Schema Issues**: Ensure database tables are created correctly

### **Logs**

```bash
# View application logs
tail -f logs/application.log

# Enable debug logging
export LOGGING_LEVEL_COM_LINEAGE=DEBUG
```

## 📁 **Repository Files**

This repository includes the following key files for deployment and testing:

- **`DEPLOYMENT.md`**: Complete deployment guide with Git setup instructions
- **`TESTING.md`**: Comprehensive testing guide with API examples  
- **`schema.sql`**: PostgreSQL database schema with all tables and indexes
- **`docker-compose.yml`**: Docker Compose configuration for easy deployment
- **`Dockerfile`**: Container configuration for the backend service
- **`env.template`**: Environment variables template
- **`.gitignore`**: Git ignore file for Java/Maven projects

## 🔗 **Related Documentation**

- [📋 Deployment Guide](./DEPLOYMENT.md) - Complete setup instructions
- [🧪 Testing Guide](./TESTING.md) - API testing and validation
- [🗄️ Database Schema](./schema.sql) - PostgreSQL table definitions

## 📄 **License**

This project is licensed under the MIT License. 
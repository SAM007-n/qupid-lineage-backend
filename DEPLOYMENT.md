# 🚀 QupidLineage Backend Deployment Guide

## 📋 Prerequisites

Before deploying the backend, ensure you have:
- Java 17+ installed
- Maven 3.6+ installed
- Docker and Docker Compose installed
- PostgreSQL database access
- Git installed

## 🔧 Git Repository Setup

### 1. Initialize Git Repository

```bash
# Navigate to the backend directory
cd backend

# Initialize git repository
git init

# Add all files
git add .

# Create initial commit
git commit -m "Initial commit: QupidLineage Backend API"

# Add remote repository (replace with your GitHub repo URL)
git remote add origin https://github.com/YOUR_USERNAME/qupid-lineage-backend.git

# Push to GitHub
git branch -M main
git push -u origin main
```

### 2. Create .gitignore

Create a `.gitignore` file in the backend directory:

```gitignore
# Maven
target/
pom.xml.tag
pom.xml.releaseBackup
pom.xml.versionsBackup
pom.xml.next
release.properties
dependency-reduced-pom.xml
buildNumber.properties
.mvn/timing.properties
.mvn/wrapper/maven-wrapper.jar

# IDE
.idea/
*.iws
*.iml
*.ipr
.vscode/
.eclipse/

# OS
.DS_Store
Thumbs.db

# Logs
logs/
*.log

# Environment variables
.env
.env.local
.env.production
.env.test

# Application specific
application-local.yml
application-dev.yml
```

## 🗄️ Database Setup

### PostgreSQL Database Schema

Run the following SQL script to create the database schema:

```sql
-- Create database
CREATE DATABASE lineage_extractor;

-- Connect to the database
\c lineage_extractor;

-- Create extraction_runs table
CREATE TABLE extraction_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repository_url VARCHAR(500) NOT NULL,
    branch VARCHAR(100) NOT NULL DEFAULT 'main',
    commit_hash VARCHAR(100),
    commit_timestamp TIMESTAMP,
    run_mode VARCHAR(20) NOT NULL DEFAULT 'FULL',
    phase VARCHAR(20) NOT NULL DEFAULT 'STARTED',
    triggered_by VARCHAR(100) NOT NULL,
    extractor_version VARCHAR(50) NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    stats JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create files table
CREATE TABLE files (
    file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    file_path VARCHAR(500) NOT NULL,
    file_type VARCHAR(20) NOT NULL,
    file_url VARCHAR(500),
    file_hash VARCHAR(64) NOT NULL,
    last_modified_at TIMESTAMP,
    extracted_at TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    raw_sql_content TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create tables table
CREATE TABLE tables (
    table_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    table_role VARCHAR(50) NOT NULL,
    table_schema VARCHAR(100),
    columns JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create lineage_edges table
CREATE TABLE lineage_edges (
    edge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    edge_type VARCHAR(50) NOT NULL,
    from_table VARCHAR(255) NOT NULL,
    from_column VARCHAR(255),
    to_table VARCHAR(255) NOT NULL,
    to_column VARCHAR(255),
    transformation_type VARCHAR(100) NOT NULL,
    transformation_lines JSONB,
    transformation_code TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create job_status table
CREATE TABLE job_status (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    pod_id VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    total_files INTEGER NOT NULL DEFAULT 0,
    processed_files INTEGER NOT NULL DEFAULT 0,
    succeeded_files INTEGER NOT NULL DEFAULT 0,
    failed_files INTEGER NOT NULL DEFAULT 0,
    current_file VARCHAR(500),
    processing_speed DECIMAL(10,2),
    estimated_time_remaining DECIMAL(10,2),
    current_phase VARCHAR(100),
    error_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_extraction_runs_repo_url ON extraction_runs(repository_url);
CREATE INDEX idx_extraction_runs_created_at ON extraction_runs(created_at DESC);
CREATE INDEX idx_files_run_id ON files(run_id);
CREATE INDEX idx_files_file_path ON files(file_path);
CREATE INDEX idx_tables_file_id ON tables(file_id);
CREATE INDEX idx_tables_table_name ON tables(table_name);
CREATE INDEX idx_lineage_edges_file_id ON lineage_edges(file_id);
CREATE INDEX idx_lineage_edges_from_table ON lineage_edges(from_table);
CREATE INDEX idx_lineage_edges_to_table ON lineage_edges(to_table);
CREATE INDEX idx_job_status_run_id ON job_status(run_id);

-- Add trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_extraction_runs_updated_at BEFORE UPDATE
    ON extraction_runs FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

## 🐳 Docker Setup

### 1. Create docker-compose.yml for Development

Create a `docker-compose.yml` file in the backend directory:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    container_name: lineage-postgres
    environment:
      POSTGRES_DB: lineage_extractor
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: lineage123
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./schema.sql:/docker-entrypoint-initdb.d/schema.sql
    networks:
      - lineage-network

  backend:
    build: .
    container_name: lineage-backend
    ports:
      - "8080:8080"
    environment:
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/lineage_extractor
      - SPRING_DATASOURCE_USERNAME=postgres
      - SPRING_DATASOURCE_PASSWORD=lineage123
      - GROQ_API_KEY=${GROQ_API_KEY}
      - GITHUB_TOKEN=${GITHUB_TOKEN}
    depends_on:
      - postgres
    networks:
      - lineage-network

volumes:
  postgres_data:

networks:
  lineage-network:
    driver: bridge
```

### 2. Create Dockerfile

Create a `Dockerfile` in the backend directory:

```dockerfile
FROM openjdk:17-jdk-slim

WORKDIR /app

# Copy maven files
COPY pom.xml .
COPY .mvn .mvn
COPY mvnw .

# Download dependencies
RUN ./mvnw dependency:go-offline -B

# Copy source code
COPY src src

# Build application
RUN ./mvnw clean package -DskipTests

# Expose port
EXPOSE 8080

# Run application
CMD ["java", "-jar", "target/sql-dependency-extractor-backend-1.0.0.jar"]
```

## 🔧 Environment Configuration

### 1. Create .env.template

Create a `.env.template` file:

```bash
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=lineage_extractor
POSTGRES_USER=postgres
POSTGRES_PASSWORD=lineage123

# API Keys
GROQ_API_KEY=your-groq-api-key-here
GITHUB_TOKEN=your-github-token-here

# Docker Configuration
DOCKER_IMAGE_NAME=qupid-lineage
DOCKER_BACKEND_URL=http://host.docker.internal:8080/api

# Application Configuration
SERVER_PORT=8080
SPRING_PROFILES_ACTIVE=dev
```

### 2. Create application-dev.yml

Create `src/main/resources/application-dev.yml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5432}/${POSTGRES_DB:lineage_extractor}
    username: ${POSTGRES_USER:postgres}
    password: ${POSTGRES_PASSWORD:lineage123}
  jpa:
    hibernate:
      ddl-auto: update
    show-sql: true

logging:
  level:
    com.lineage: DEBUG
    org.springframework.web: DEBUG
```

## 🚀 Quick Start for Testers

### Option 1: Using Docker Compose (Recommended)

1. **Clone the repository:**
```bash
git clone https://github.com/YOUR_USERNAME/qupid-lineage-backend.git
cd qupid-lineage-backend
```

2. **Set up environment variables:**
```bash
cp .env.template .env
# Edit .env file with your actual API keys
```

3. **Start the application:**
```bash
docker-compose up -d
```

4. **Wait for services to start (about 30 seconds), then test:**
```bash
curl http://localhost:8080/api/extraction/health
```

### Option 2: Manual Setup

1. **Clone and setup database:**
```bash
git clone https://github.com/YOUR_USERNAME/qupid-lineage-backend.git
cd qupid-lineage-backend

# Start PostgreSQL
docker run --name lineage-postgres \
  -e POSTGRES_PASSWORD=lineage123 \
  -e POSTGRES_DB=lineage_extractor \
  -p 5432:5432 \
  -d postgres:15

# Run schema script
psql -h localhost -U postgres -d lineage_extractor -f schema.sql
```

2. **Set environment variables:**
```bash
export GROQ_API_KEY="your-groq-api-key"
export GITHUB_TOKEN="your-github-token"
```

3. **Build and run:**
```bash
./mvnw clean install
./mvnw spring-boot:run
```

## 🧪 Testing the API

### Health Check
```bash
curl http://localhost:8080/api/extraction/health
```

### Start Repository Scan
```bash
curl -X POST http://localhost:8080/api/extraction/scan \
  -H "Content-Type: application/json" \
  -d '{
    "repositoryUrl": "https://github.com/dbt-labs/jaffle_shop",
    "branch": "main",
    "triggeredBy": "test-user"
  }'
```

### Get Extraction Runs
```bash
curl http://localhost:8080/api/extraction/runs
```

### Get Specific Run Status
```bash
curl http://localhost:8080/api/extraction/run/{runId}
```

## 📊 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/extraction/health` | Health check |
| POST | `/api/extraction/scan` | Start repository scan |
| GET | `/api/extraction/runs` | List all extraction runs |
| GET | `/api/extraction/run/{runId}` | Get specific run details |
| POST | `/api/extraction/run/{runId}/cancel` | Cancel extraction run |
| POST | `/api/extraction/webhook` | Webhook endpoint for extraction pods |

## 🔍 Troubleshooting

### Common Issues

1. **Port 8080 already in use:**
   - Change `SERVER_PORT` in `.env` file
   - Or kill the process: `lsof -ti:8080 | xargs kill -9`

2. **Database connection failed:**
   - Ensure PostgreSQL is running: `docker ps | grep postgres`
   - Check database logs: `docker logs lineage-postgres`

3. **Build failures:**
   - Ensure Java 17+ is installed: `java -version`
   - Clean and rebuild: `./mvnw clean install -U`

### Logs

```bash
# View application logs
docker logs lineage-backend -f

# View database logs
docker logs lineage-postgres -f

# View all services
docker-compose logs -f
```

## 📝 Development

### Running Tests
```bash
./mvnw test
```

### Hot Reload (Development)
```bash
./mvnw spring-boot:run -Dspring.profiles.active=dev
```

### Building for Production
```bash
./mvnw clean package -Dspring.profiles.active=prod
```

## 🔐 Security Notes

- The current setup uses basic security for development
- For production, implement proper authentication and authorization
- Use environment-specific configuration files
- Never commit sensitive information like API keys to Git

## 📞 Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review application logs
3. Ensure all prerequisites are met
4. Create an issue in the GitHub repository with detailed error information

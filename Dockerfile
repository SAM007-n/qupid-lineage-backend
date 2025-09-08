FROM openjdk:17-jdk-slim

# Install curl for health checks
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy maven wrapper
COPY .mvn .mvn
COPY mvnw .
COPY pom.xml .

# Make mvnw executable
RUN chmod +x ./mvnw

# Download dependencies
RUN ./mvnw dependency:go-offline -B

# Copy source code
COPY src src

# Build application
RUN ./mvnw clean package -DskipTests

# Create non-root user
RUN useradd -r -s /bin/false lineage && \
    chown -R lineage:lineage /app

# Switch to non-root user
USER lineage

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/api/extraction/health || exit 1

# Run application
CMD ["java", "-jar", "target/sql-dependency-extractor-backend-1.0.0.jar"]

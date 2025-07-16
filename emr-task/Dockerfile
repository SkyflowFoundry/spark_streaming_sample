# =============================================================================
# Spark Job Dockerfile
# =============================================================================
# 
# PURPOSE:
#   This Dockerfile creates a container for running the Spark streaming job.
#   It builds the emr-task module and sets up the Spark environment with
#   necessary dependencies for S3 and Hudi operations.
#
# BUILD PROCESS:
#   - Multi-stage build using Maven for compilation
#   - Final stage uses Bitnami Spark image for runtime
#   - Installs additional utilities (bash, coreutils) for wait scripts
#
# RUNTIME:
#   - Executes run-spark-job.sh as the entrypoint
#   - Waits for Spark master and Kafka to be ready
#   - Runs the Spark streaming job with S3/Hudi configuration
#
# DEPENDENCIES:
#   - Requires the entire project to be copied (for Maven dependencies)
#   - Uses parent POM for proper module resolution
#
# FOR MORE INFORMATION:
#   See the main README.md file for complete setup and usage instructions.
# =============================================================================

FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
# Copy the entire project (parent POM and all modules)
COPY . .
# First install all modules, then build the specific module with assembly
RUN mvn clean install && mvn package -pl emr-task

FROM bitnami/spark:3.5
# Install bash and coreutils for the wait script
USER root
RUN apt-get update && apt-get install -y bash coreutils netcat-openbsd && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/app
COPY --from=build /app/emr-task/target/emr-task-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./
COPY emr-task/run-spark-job.sh ./
RUN chmod +x run-spark-job.sh
ENTRYPOINT ["./run-spark-job.sh"] 
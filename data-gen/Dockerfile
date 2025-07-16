# =============================================================================
# Kafka Producer Dockerfile
# =============================================================================
# 
# PURPOSE:
#   This Dockerfile creates a container for running the Kafka producer.
#   It builds the data-gen module and sets up the Java runtime environment
#   for generating and publishing synthetic data to Kafka topics.
#
# BUILD PROCESS:
#   - Multi-stage build using Maven for compilation
#   - Final stage uses Amazon Corretto Java runtime
#   - Installs additional utilities (bash, coreutils) for wait scripts
#
# RUNTIME:
#   - Executes run-kafka-producer.sh as the entrypoint
#   - Waits for Kafka to be ready before starting
#   - Generates and publishes synthetic customer data to Kafka
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
RUN mvn clean install && mvn package -pl data-gen

FROM amazoncorretto:17-alpine
# Install bash and coreutils for the wait script
RUN apk add --no-cache bash coreutils
WORKDIR /usr/src/app
COPY --from=build /app/data-gen/target/SyntheticDataGenerator-1.0.0-SNAPSHOT-jar-with-dependencies.jar ./
COPY data-gen/run-kafka-producer.sh ./
RUN chmod +x run-kafka-producer.sh
ENTRYPOINT ["./run-kafka-producer.sh"] 
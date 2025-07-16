#!/bin/sh
# =============================================================================
# Kafka Producer Runner Script
# =============================================================================
# 
# PURPOSE:
#   This script runs the Kafka producer that:
#   - Generates synthetic customer data
#   - Publishes data to Kafka topics for processing
#   - Can be run manually to inject data into the streaming pipeline
#   - Supports AWS credentials for any AWS-dependent operations
#
# USAGE:
#   This script is executed by the kafka-producer Docker container.
#   The container is NOT started automatically by docker-compose.
#   To run the producer manually:
#     docker-compose run --rm kafka-producer
#
# CONFIGURATION:
#   - Environment variables are injected from docker-compose.yml
#   - Kafka connection details are provided via environment variables
#   - AWS credentials are available for any AWS operations
#
# DEPENDENCIES:
#   - Kafka must be running and accessible
#   - AWS credentials must be valid (validated by aws-credentials-check)
#
# FOR MORE INFORMATION:
#   See the main README.md file for complete setup and usage instructions.
# =============================================================================

set -e

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
while ! timeout 1 bash -c "</dev/tcp/kafka/9092" 2>/dev/null; do
  echo "Waiting for Kafka at kafka:9092..."
  sleep 3
done

echo "Kafka is up. Starting Kafka producer..."

exec java -cp /usr/src/app/data-gen-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  com.skyflow.sample.KafkaPublisher \
  "$KAFKA_BOOTSTRAP_SERVERS" \
  "$KAFKA_TOPIC" \
  100 
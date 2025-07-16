#!/bin/sh
# =============================================================================
# Spark Job Runner Script
# =============================================================================
# 
# PURPOSE:
#   This script runs the main Spark streaming job that:
#   - Reads data from Kafka topics
#   - Processes data through Skyflow Vault API for tokenization
#   - Writes processed data to S3 using Apache Hudi format
#   - Handles AWS credentials and S3 filesystem configuration
#
# USAGE:
#   This script is executed automatically by the spark-job Docker container
#   as defined in docker-compose.yml. It waits for dependencies (Spark master,
#   Kafka) to be ready before starting the Spark job.
#
# CONFIGURATION:
#   - Environment variables are injected from docker-compose.yml
#   - AWS credentials are provided via environment variables
#   - S3 filesystem is configured for Hudi table writes
#
# DEPENDENCIES:
#   - Spark master must be running and accessible
#   - Kafka must be running and accessible
#   - AWS credentials must be valid (validated by aws-credentials-check)
#
# FOR MORE INFORMATION:
#   See the main README.md file for complete setup and usage instructions.
# =============================================================================

set -e

# Wait for Spark master
echo "Waiting for Spark master to be ready..."
while ! timeout 1 bash -c "</dev/tcp/spark-master/7077" 2>/dev/null; do
  echo "Waiting for Spark master at spark-master:7077..."
  sleep 3
done

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
while ! timeout 1 bash -c "</dev/tcp/kafka/9092" 2>/dev/null; do
  echo "Waiting for Kafka at kafka:9092..."
  sleep 3
done

echo "Spark master and Kafka are up. Submitting Spark job..."

exec /opt/bitnami/spark/bin/spark-submit \
  --master "$SPARK_MASTER" \
  --jars /usr/src/app/emr-task-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  --repositories https://repo1.maven.org/maven2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1 \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain" \
  --conf "spark.hadoop.fs.s3a.endpoint=s3.$AWS_REGION.amazonaws.com" \
  --conf "spark.hadoop.fs.s3a.path.style.access=false" \
  --class com.skyflow.sample.EmrTask \
  /usr/src/app/emr-task-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
  true \
  com.skyflow.sample.Customer \
  1 \
  5 \
  "$S3_BUCKET" \
  customertbl \
  kafka:9092 \
  local \
  5 \
  5 \
  "$AWS_REGION" \
  "$AWS_SECRET_NAME_FOR_SA_API_KEY" \
  "$VAULT_ID" \
  "$VAULT_URL" \
  20 \
  false \
  "(console)" \
  10 
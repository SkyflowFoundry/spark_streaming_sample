# Project Setup and Running Instructions

## Quick Start: Build Everything with Docker (No Java/Maven Needed Locally)

This project is now a Maven multi-module build. You can build all modules at once using Docker:

```sh
docker run --rm -v "$PWD":/usr/src/mymaven -w /usr/src/mymaven maven:3.9.6-eclipse-temurin-17 mvn clean install
```
- This will compile, test, and package all modules using the top-level parent POM.
- No need to install Java or Maven on your machine.

## Multi-Module Maven Structure
- The top-level `pom.xml` is a parent POM that aggregates all modules: `utils`, `data-model`, `data-gen`, and `emr-task`.
- All modules inherit configuration and dependency versions from the parent.
- Inter-module dependencies are managed automatically.
- You can still build individual modules as before (see below), but the recommended approach is to use the top-level build.

---

## Requirements (for advanced/manual builds)
- JDK version 1.8. strictly 1.8. Don't get a later version. Or earlier.
- Maven
- Vault (schema in `vaultSchema.json`)
- Service accounts with Vault Editor role
- AWS Account with sufficient privileges (even if you run locally, you need AWS secrets manager)
- Docker (if you wanna test locally; non-needed otherwise)

---

## Project Modules

### utils
**Build Process:**
```bash
cd utils/
mvn clean
mvn package install
```

### data-model
**Build Process:**
```bash
cd data-model/
mvn clean
mvn package install
```

### data-generator
**Build Process:**
```bash
cd data-gen/
mvn clean
mvn package install assembly:single
```
Resulting JAR: `target/SyntheticDataGenerator-1.0-SNAPSHOT-jar-with-dependencies.jar`. This has a whole bunch of tools that you can run from the CLI. Each has its own usage. Best bet is to check out source code.

### emr-task
**Build Process:**
```bash
mvn package assembly:single
```
Resulting JAR: `target/emr-task-<version>-jar-with-dependencies.jar`

## Running Full Pipeline Locally
In this case, we want to run the Kafka -> Spark pipeline, all locally.

### Kafka Setup
First, we need Kafka. We will setup a local cluster using docker. From workspace top, exec:
```bash
docker-compose -d up
```
This will give you a bootstrap server at "localhost:29092".
You will also get a container named 'kafka' for running kafka CLI scripts.
Or you may have scripts locally. In the examples below, I'm assuming use of docker. Adjust to taste.

Next, we will create a topic, called 'local' to test locally.
```bash
docker-compose exec kafka /bin/kafka-topics --create --bootstrap-server kafka:9092 --topic local
```

Now, we can interact with the topic via kafka cli commands. e.g. to run kafka console producer:
```bash
docker-compose exec -it kafka /bin/kafka-console-producer --bootstrap-server localhost:29092 --topic local
```
Type your kakfa messages on the prompt; ^C exits -- standard kafka console behavior. Note, in the above `kafka:9092` is the docker local equivalent of `l`ocalhost:29092`.

### Spark setup
Now that you have Kafka, you can focus on the Spark part. The Spark part will tokenize
using the vault (unless you skip skyflow). So we first create a vault and a service-account (Vault Editor role)
with API key. Drop the API key in AWS Secrets manager. Configure AWS credentials, including region.

I like to set the following env var:
```bash
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=60"
```

You must have the following setup:
```bash
export SPARK_LOCAL_IP=localhost
export JAVA_HOME=<...>
```

Now, we running Spark job
```bash
spark-submit \
    --jars emr-task/target/emr-task-1.0.0-jar-with-dependencies.jar \
    --repositories https://repo1.maven.org/maven2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    --class com.skyflow.sample.EmrTask \
    emr-task/target/emr-task-1.0.0.jar \
      true \
      com.skyflow.sample.Customer \
      `pwd`/hudi customertbl \
      localhost:29092 local 5 5 \
      ${AWS_REGION} ${AWS_SECRET_NAME_FOR_SA_API_KEY} \
      ${VAULT_ID} ${VAULT_URL} 20 false \
      "(console)" 10
```

To understand the above there are arguments that spark consumes (read `spark-submit` tutorial / help). The arguments after `emr-task-<version>.jar` go straight to the EmrTask which will print usage if you run it without arguments. For the above call, here are the arguments, in order:
### Pipeline Configuration
- `<localIO>`: Boolean flag for local input/output
- `<full.java.class>`: Fully qualified Java class name for processing

### Output Configuration
- `<output-s3-bucket>`: S3 bucket for output storage
- `<table-name>`: Table name for processing

### Input Configuration
- `<kafka-bootstrap>`: Kafka bootstrap server address
- `<kafka-topic>`: Kafka topic to subscribe
- `<kafka-batch-size>`: Batch processing size
- `<batch-delay-secs>`: Microbatch delay in seconds

### Vault Configuration
- `<aws-region>`: AWS resource region
- `<secret-name>`: AWS Secrets Manager secret name for vault API key
- `<vault-id>`: Vault ID
- `<vault-url>`: Vault URL
- `<vault-batch-size>`: Vault operations batch size
- `<short-circuit-skyflow?>`: Boolean to short-circuit Skyflow (useful for before/after testing)

### Metrics Configuration
- `<namespace>`: CloudWatch namespace for metrics
- `<reporting-delay-secs>`: Metrics reporting delay

# End-to-End Local Setup with Docker Compose

This project supports a full local build and run using Docker Compose, including Kafka (with topic auto-creation), Spark (standalone), and automated job execution. Vault must be remote; you must provide your own AWS and Vault details.

## Prerequisites

- Docker and Docker Compose installed
- AWS account with access to Secrets Manager
- Remote Vault instance with API key stored in AWS Secrets Manager
- Valid AWS credentials (access key, secret key, session token for temporary credentials)

## Steps

1. **Create Vault & Service account**
Create a vault using the schema given in vaultSchema.json.  This works for the old style vaults.
Also create a service account with permissions to write to the vault & create tokens (Vault
Editor role works nicely). Get an API key for this service account.

1. **Create AWS setup**
In AWS create a secret in secrets manager. Store the Service Account API Key from above in *plaintext*.

Create an S3 bucket to write into.

Create an IAM User or other credentials that can read the secret, and write to this S3 bucket. Note its AWS credentials.

1. **Configure Environment Variables**
   - Create a `.env` file at the project root with the following variables:
     ```
     # AWS Configuration
     AWS_REGION=us-west-2
     AWS_ACCESS_KEY_ID=your_access_key_id
     AWS_SECRET_ACCESS_KEY=your_secret_access_key
     AWS_SESSION_TOKEN=your_session_token  # Required for temporary credentials
     
     # Vault Configuration
     AWS_SECRET_NAME_FOR_SA_API_KEY=your_secret_name_in_aws_secrets_manager
     VAULT_ID=your_vault_id
     VAULT_URL=https://your_vault_url
     
     # S3 Configuration
     S3_BUCKET=your_s3_bucket_name
     ```
   - **Important**: The secret in AWS Secrets Manager should contain just the API key (not JSON)
   - For temporary credentials (like those from AWS SSO), all three AWS credential variables are required

2. **Build All Modules**
   - Run the helper script to build all Maven modules:
     ```sh
     ./build-all.sh
     ```

3. **Start the System**
   - Bring up all services:
     ```sh
     docker-compose up --build
     ```
   - This will:
     - **Validate AWS credentials** first (via `aws-credentials-check` service)
     - Start Zookeeper, Kafka, Spark Master, Spark Worker
     - Create the Kafka topic `local` automatically (via the `kafka-init` service)
     - Build and run the Spark job (from `emr-task`), which will wait for data from Kafka
   - If AWS credentials are invalid, the system will fail fast and show the error

4. **Manually Run the Kafka Producer**
   - The producer is NOT started automatically. To produce data to Kafka, run:
     ```sh
     docker-compose run --rm kafka-producer
     ```
   - You can run this as many times as you want to send more data.

5. **Viewing Output & Logs**
   - To see logs for any service:
     ```sh
     docker-compose logs -f kafka-producer
     docker-compose logs -f spark-job
     docker-compose logs -f aws-credentials-check
     ```
   - To see all logs together:
     ```sh
     docker-compose logs -f
     ```
   - To access a running container for debugging:
     ```sh
     docker-compose exec spark-job sh
     ```
   - Spark UI: [http://localhost:8080](http://localhost:8080)
   - Kafka: Exposed on `localhost:29092`

6. **Troubleshooting**
   - **AWS Credentials Issues**: The `aws-credentials-check` service validates credentials before other services start
   - **Environment Variable Changes**: If you update your `.env` file, restart with `docker-compose down && docker-compose up --build`
   - **Container Restarts**: Individual services can be restarted with `docker-compose restart <service-name>`
   - **Build Issues**: Rebuild with `./build-all.sh` if you modify Java code

7. **Notes**
   - The Kafka topic is created automatically at startup
   - The Spark job will wait for Kafka input and process data as it arrives
   - The Kafka producer is run manually, on demand, using `docker-compose run --rm kafka-producer`
   - You can customize the producer or job scripts as needed in `data-gen/run-kafka-producer.sh` and `emr-task/run-spark-job.sh`

---

### AWS Credentials Setup

This project expects AWS credentials to be available to the containers. The system includes an automatic AWS credentials validation service that runs before other services start.

## Environment Variables (Recommended)

Add the following to your `.env` file:
```
# AWS Configuration
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_SESSION_TOKEN=your_session_token  # Required for temporary credentials

# Vault Configuration
AWS_SECRET_NAME_FOR_SA_API_KEY=your_secret_name_in_aws_secrets_manager
VAULT_ID=your_vault_id
VAULT_URL=https://your_vault_url

# S3 Configuration
S3_BUCKET=your_s3_bucket_name
```

**Important Notes:**
- **Temporary Credentials**: If using temporary credentials (AWS SSO, STS, etc.), all three AWS credential variables are required
- **Secret Format**: The secret in AWS Secrets Manager should contain just the API key string, not JSON
- **Validation**: The `aws-credentials-check` service validates credentials using `aws sts get-caller-identity` before starting other services

### Output

The above process creates Hudi tables in the S3 locations give. You can read these like normal Hudi tables.

# Advanced Usage (Explore on your own)

You can also run this in various other combinations. e.g. you can run this on a proper, large-scale AWS MSK cluster
and Spark Serverless job. Or, you can first pre-populate the vault with some data and then run a mix of updates and
inserts of specified proportions.

This also can be configured to log metrics to Cloudwatch.
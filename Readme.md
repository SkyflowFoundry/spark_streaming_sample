# Project Setup and Running Instructions

## Requirements
- JDK version 1.8. strictly 1.8. Don't get a later version. Or earlier.
- Maven
- Vault (schema in `vaultSchema.json`)
- Service accounts with Vault Editor role
- AWS Account with sufficient privileges (even if you run locally, you need AWS secrets manager)
- Docker (if you wanna test locally; non-needed otherwise)

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
    --class com.skyflow.walmartpoc.EmrTask \
    emr-task/target/emr-task-1.0.0.jar \
      true \
      com.skyflow.walmartpoc.Customer \
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
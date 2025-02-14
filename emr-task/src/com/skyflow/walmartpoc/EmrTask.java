package com.skyflow.walmartpoc;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import com.skyflow.utils.ReflectionUtils;
import com.skyflow.utils.ReflectionUtils.VaultObjectInfo;

public class EmrTask {
    private static Logger logger = LogManager.getLogger("myLogger");
    
    private static class VaultObjectInfoCache {
        private static volatile VaultObjectInfoCache instance;
        private final Map<Class<?>, VaultObjectInfo<?>> vaultObjectInfoMap;

        private VaultObjectInfoCache() {
            vaultObjectInfoMap = new HashMap<>();
        }

        public static VaultObjectInfoCache getInstance() {
            if (instance == null) {
                synchronized (VaultObjectInfoCache.class) {
                    if (instance == null) {
                        instance = new VaultObjectInfoCache();
                    }
                }
            }
            return instance;
        }

        @SuppressWarnings("unchecked")
        public synchronized <T> VaultObjectInfo<T> getVaultObjectInfo(Class<T> clazz) {
            return (VaultObjectInfo<T>) vaultObjectInfoMap.computeIfAbsent(clazz, ReflectionUtils::getVaultObjectInfo);
        }
    }

    private static class BatchProcessor<T extends SerializableDeserializable> implements MapPartitionsFunction<Row, Row> {
        private final String cloudwatchNamespace;
        private final int reportingDelaySecs;
        private final Class<T> clazz;
        private final int vaultBatchSize;
        private final String vault_id;
        private final String vault_url;
        private final String credentialString;
        private final boolean shortCircuitSkyflow;
        private final Semaphore semaphore;

        public BatchProcessor(String cloudwatchNamespace, int reportingDelaySecs, Class<T> clazz,
                                int vaultBatchSize, int maxOutstandingRequests,
                                String vault_id, String vault_url, String credentialString, boolean shortCircuitSkyflow) {
            this.cloudwatchNamespace = cloudwatchNamespace;
            this.reportingDelaySecs = reportingDelaySecs;
            this.clazz = clazz;
            this.vaultBatchSize = vaultBatchSize;
            this.vault_id = vault_id;
            this.vault_url = vault_url;
            this.credentialString = credentialString;
            this.shortCircuitSkyflow = shortCircuitSkyflow;
            this.semaphore = new Semaphore(maxOutstandingRequests);
        }

        @Override
        public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
            String partitionId = "TaskId:"+TaskContext.getPartitionId() + "-" + java.util.UUID.randomUUID().toString();
            logger.info("PART-START " + partitionId + " Starting partition");

            try (final CollectorAndReporter stats = new CollectorAndReporter(cloudwatchNamespace, reportingDelaySecs*1000)) {

                Map<String, String> dimensionMap = new HashMap<>();
                dimensionMap.put("object", clazz.getSimpleName());
                ValueDatum numRecords = stats.createOrGetUniqueMetric("emrRecordsProcessed", dimensionMap, StandardUnit.COUNT, ValueDatum.class);
                ValueDatum numSkyflowBatches = stats.createOrGetUniqueMetric("numSkyflowBatches", dimensionMap, StandardUnit.COUNT, ValueDatum.class);
                ValueDatum numKafkaBatches = stats.createOrGetUniqueMetric("numKafkaBatches", dimensionMap, StandardUnit.COUNT, ValueDatum.class);
                StatisticDatum skyflowBatchSizes = stats.createOrGetUniqueMetric("skyflowBatchSizes", dimensionMap, StandardUnit.COUNT, StatisticDatum.class);
                StatisticDatum kafkaBatchSizes = stats.createOrGetUniqueMetric("kafkaBatchSizes", dimensionMap, StandardUnit.COUNT, StatisticDatum.class);
                StatisticDatum apiLatency = stats.createOrGetUniqueMetric("apiLatency", dimensionMap, StandardUnit.MILLISECONDS, StatisticDatum.class);
                ValueDatum apiErrors = stats.createOrGetUniqueMetric("apiErrors", dimensionMap, StandardUnit.COUNT, ValueDatum.class);

                numKafkaBatches.increment();
                long kafkaBatchSize=0;
                List<CompletableFuture<List<Row>>> futures = new ArrayList<>();
                CloseableHttpAsyncClient httpClient = HttpAsyncClients.createDefault();
                httpClient.start();
                while (iterator.hasNext()) {
                    List<String> batch = new ArrayList<>();
                    for (int i = 0; i < vaultBatchSize && iterator.hasNext(); i++) {
                        numRecords.increment();
                        kafkaBatchSize++;
                        batch.add(iterator.next().getString(0)); // There is only one column in the Row: valueString
                    }
                    if (!batch.isEmpty()) {
                        numSkyflowBatches.increment();
                        skyflowBatchSizes.value(batch.size());

                        semaphore.acquire();

                        futures.add(getTokenizedObjects(
                            partitionId,
                            apiLatency,
                            apiErrors,
                            batch.toArray(new String[0]),
                            httpClient
                        ));
                    }
                    stats.pollAndReport(false);
                }
                kafkaBatchSizes.value(kafkaBatchSize);
                logger.info("PART-END " + partitionId + " Processed " + futures.size() + " skyflow batches in partition");
                
                return futures.stream()
                    .map(CompletableFuture::join)
                    .flatMap(List::stream)
                    .collect(Collectors.toList())
                    .iterator();
            }
        }

        private CompletableFuture<List<Row>> getTokenizedObjects(String partitionId, StatisticDatum apiLatency, ValueDatum apiError, String[] jsons, CloseableHttpAsyncClient httpClient) throws Exception {
            try {
                VaultObjectInfoCache cache = VaultObjectInfoCache.getInstance();
                VaultObjectInfo<T> objectInfo = cache.getVaultObjectInfo(clazz);
                return _getTokenizedObjects(partitionId, apiLatency, apiError, jsons, objectInfo, httpClient);
            } catch (Exception e) {
                throw new Exception("NOT-FOR-PRODUCTION: Failed to process: " + String.join(", ", jsons), e); // THIS LOGS PII. OBVIOUSLY NOT FOR PRODUCTION USE
            }
        }

        @SuppressWarnings("unchecked")
        private CompletableFuture<List<Row>> _getTokenizedObjects(String partitionId, StatisticDatum apiLatency, ValueDatum apiError, String[] jsons, VaultObjectInfo<T> objectInfo, CloseableHttpAsyncClient httpClient) throws Exception {
            CompletableFuture<List<Row>> future = new CompletableFuture<>();
            if (!shortCircuitSkyflow) {
                T[] objArray = (T[]) new SerializableDeserializable[jsons.length];

                JSONArray recordsArray = new JSONArray();

                int i;
                i=0;
                for (String json : jsons) {
                    //System.out.println(" ("+ partitionId + ") recv: " + json);
                    T obj = clazz.getConstructor(String.class).newInstance(json);
                    objArray[i] = obj;
                    JSONObject record = new JSONObject();
                    record.put("table", objectInfo.tableName);
    
                    JSONObject fields = ReflectionUtils.jsonObjectForVault(obj, objectInfo);
                    record.put("fields", fields);
                    recordsArray.add(record);
                    i = i + 1;
                }
    
                String insertRecordUrl = vault_url + "/v1/vaults/" + vault_id + "/" + objectInfo.tableName;
                JSONObject insertReqBody = new JSONObject();
                insertReqBody.put("records", recordsArray);
                insertReqBody.put("tokenization", true);
                insertReqBody.put("upsert", objectInfo.upsertColumnName);

                SimpleHttpRequest request = SimpleRequestBuilder.post(insertRecordUrl).build();
                request.setHeader("Authorization", "Bearer " + credentialString);
                request.setBody(insertReqBody.toJSONString(), ContentType.APPLICATION_JSON);
                
                final String requestId = java.util.UUID.randomUUID().toString();
                logger.info("API-REQ " + partitionId + " " + requestId + " emitting request");
                long startTime = System.currentTimeMillis();

                httpClient.execute(request, new FutureCallback<SimpleHttpResponse>() {
                    @Override
                    public void completed(SimpleHttpResponse response) {
                        long latency = (System.currentTimeMillis() - startTime);
                        semaphore.release();

                        logger.info("API-RESP-COMPL " + partitionId + " " + requestId + " " + latency + " (ms) recvd response for request " + requestId);
                        apiLatency.value(latency);

                        String bodyString = null;

                        if (response.getCode() != 200) {
                            String statusLine = response.getReasonPhrase();
                            bodyString = response.getBodyText();
                            apiError.increment();
                            future.completeExceptionally(new Exception("Partion id: " + partitionId + " Request: " + requestId + " http response: " + statusLine + "\nResponse Body: " + bodyString + "\nRequest: " + insertReqBody.toJSONString()));
                            return;
                        }
    
                        bodyString = response.getBodyText();
                        JSONParser jsonParser = new JSONParser();
                        JSONObject insertResponse;
                        try {
                            insertResponse = (JSONObject) jsonParser.parse(bodyString);
                        } catch (ParseException e) {
                            apiError.increment();
                            future.completeExceptionally(new Exception("NOT-FOR-PRODUCTION: Failed to process: " + String.join(", ", jsons), e)); // THIS LOGS PII. OBVIOUSLY NOT FOR PRODUCTION USE);
                            return;
                        }
                        JSONArray responseRecords = (JSONArray) insertResponse.get("records");
        
                        if (responseRecords.size() != jsons.length) {
                            future.completeExceptionally(new RuntimeException("NOT-FOR-PRODUCTION: Partion id: " + partitionId + " Request: " + requestId + " Skyflow was asked to upsert " + jsons.length + " records and returned " + responseRecords.size() + " results"));
                        }
                        List<Row> resultList = new ArrayList<>();
                        for (int i = 0; i < responseRecords.size(); i++) {
                            JSONObject responseRecord = (JSONObject) responseRecords.get(i);
                            JSONObject extractedFields = (JSONObject) responseRecord.get("tokens");
                            String skyflow_id = (String) responseRecord.get("skyflow_id");
        
                            T obj = objArray[i];
                            try {
                                ReflectionUtils.replaceWithValuesFromVault(obj, extractedFields, objectInfo);
                            } catch (Exception e) {
                                apiError.increment();
                                future.completeExceptionally(new Exception("NOT-FOR-PRODUCTION: Partion id: " + partitionId + " Request: " + requestId + " Failed to process: " + String.join(", ", jsons), e)); // THIS LOGS PII. OBVIOUSLY NOT FOR PRODUCTION USE);
                                return;
                            }
        
                            String objectJson = obj.toJSONString();
                            JSONParser parser = new JSONParser();
                            JSONObject jsonObject;
                            try {
                                jsonObject = (JSONObject) parser.parse(objectJson);
                            } catch (ParseException e) {
                                apiError.increment();
                                future.completeExceptionally(new Exception("NOT-FOR-PRODUCTION: Partion id: " + partitionId + " Request: " + requestId + " Failed to process: " + String.join(", ", jsons), e)); // THIS LOGS PII. OBVIOUSLY NOT FOR PRODUCTION USE);
                                return;
                            }
                            jsonObject.put("skyflow_id", skyflow_id);
                            TreeMap<String, Object> sortedMap = new TreeMap<>(jsonObject);
                            resultList.add(RowFactory.create(sortedMap.values().toArray()));
                        }
        
                        future.complete(resultList);
                    }

                    @Override
                    public void failed(Exception ex) {
                        semaphore.release();
                        logger.info("API-RESP-FAIL " + partitionId + " " + requestId + " failed on " + requestId);
                        apiError.increment();
                        future.completeExceptionally(ex);
                    }

                    @Override
                    public void cancelled() {
                        semaphore.release();
                        logger.info("API-RESP-CANCEL " + partitionId + " " + requestId + " failed on " + requestId);
                        future.completeExceptionally(new CancellationException("Request cancelled"));
                    }
                });
            } else {
                List<Row> resultList = new ArrayList<>();
                for (String json : jsons) {
                    //System.out.println(" ("+ partitionId + ") recv: " + json);
                    T obj = clazz.getConstructor(String.class).newInstance(json);
                    String objectJson = obj.toJSONString();
                    JSONParser parser = new JSONParser();
                    JSONObject jsonObject = (JSONObject) parser.parse(objectJson);
                    jsonObject.put("skyflow_id", "skipped");
                    TreeMap<String, Object> sortedMap = new TreeMap<>(jsonObject);
                    resultList.add(RowFactory.create(sortedMap.values().toArray()));
                }

                semaphore.release();
                future.complete(resultList);
            }

            return future;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("EMRTask Version 8");

        if (args.length != 18) {
            System.err.println("Usage: " + EmrTask.class.getName() + " "
                    + "<localIO> "
                    + "<full.java.class> "
                    + "<num-processing-partitions> "
                    + "<num-concurrent-requests> "
                    + "<output-s3-bucket> "
                    + "<table-name> "
                    + "<kafka-bootstrap> "
                    + "<kafka-topic> "
                    + "<kafka-batch-size> "
                    + "<batch-delay-secs> "
                    + "<aws-region> "
                    + "<secret-name> "
                    + "<vault-id> "
                    + "<vault-url> "
                    + "<vault-batch-size> "
                    + "<short-circuit-skyflow?> "
                    + "<namespace> "
                    + "<reporting-delay-secs> "
                    );
            System.err.println("Pipeline Type:");
            System.err.println("  <localIO>               : A boolean flag to determine if input comes from plaintext kafka. Spark runs locally.");
            System.err.println("  <full.java.class>       : The fully qualified Java class name of the object being processed");
            System.err.println("Parallelism Instructions:");
            System.err.println("  <num-processing-partitions>: The number of partitions for processing. (temporarily disabled)");
            System.err.println("  <num-concurrent-requests>: The number of concurrent API requests.");
            System.err.println("Output Instructions:");
            System.err.println("  <output-s3-bucket>      : The S3 bucket where the output will be stored.");
            System.err.println("  <table-name>            : The name of the table to be used in the process.");
            System.err.println("Input Instructions:");
            System.err.println("  <kafka-bootstrap>       : The Kafka bootstrap server address.");
            System.err.println("  <kafka-topic-base>      : The base name of the Kafka topic to subscribe to. (topic = basename-Class)");
            System.err.println("  <kafka-batch-size>      : The size of each batch to be processed.");
            System.err.println("  <batch-delay-secs>      : The microbatch size in seconds");
            System.err.println("Vault Instructions:");
            System.err.println("  <aws-region>            : The AWS region where resources are located.");
            System.err.println("  <secret-name>           : The name of the secret in AWS Secrets Manager that stores the vault API key");
            System.err.println("  <vault-id>              : The ID of the vault to be accessed.");
            System.err.println("  <vault-url>             : The URL of the vault service.");
            System.err.println("  <vault-batch-size>      : The batch size for vault operations.");
            System.err.println("  <short-circuit-skyflow?>: A boolean flag to determine if Skyflow should be short-circuited.");
            System.err.println("Metrics Instructions:");
            System.err.println("  <namespace>             : The Cloudwatch namespace for the metrics. Empty for no Cloudwatch reporting");
            System.err.println("  <reporting-delay-secs>  : The delay in seconds for reporting metrics.");
            System.exit(1);
        }

        boolean localIO = Boolean.parseBoolean(args[0]);
        @SuppressWarnings("unchecked")
        Class<? extends SerializableDeserializable> clazz = (Class<? extends SerializableDeserializable>) Class.forName(args[1]);
        int numProcessingPartitions = Integer.parseInt(args[2]);
        int numConcurrentRequests = Integer.parseInt(args[3]);
        String outputBucket = args[4];
        String tableName = args[5];
        String kafkaBootstrap = args[6];
        String kafkaTopic = String.format("%s-%s",args[7],clazz.getSimpleName());
        int kafkaBatchSize = Integer.parseInt(args[8]);
        int microBatchSeconds = Integer.parseInt(args[9]);
        String awsRegion = args[10];
        String secretName = args[11];
        String vault_id = args[12];
        String vault_url = args[13];
        int vaultBatchSize = Integer.parseInt(args[14]);
        boolean shortCircuitSkyflow = Boolean.parseBoolean(args[15]);
        String cloudwatchNamespace = args[16];
        int reportingDelaySecs = Integer.parseInt(args[17]);
        // For sanity checking, print out all the gathered args
        System.out.println("Pipeline Type:");
        System.out.println("  Local IO: " + localIO);
        System.out.println("  Class: " + clazz.getName());
        System.err.println("Parallelism Instructions:");
        System.out.println("  Num Processing Partitions: " + numProcessingPartitions);
        System.out.println("  Num Concurrent Requests: " + numConcurrentRequests);
        System.out.println("Output Instructions:");
        System.out.println("  Output Bucket: " + outputBucket);
        System.out.println("  Table Name: " + tableName);
        System.out.println("Input Instructions:");
        System.out.println("  Kafka Bootstrap: " + kafkaBootstrap);
        System.out.println("  Kafka Topic: " + kafkaTopic);
        System.out.println("  Kafka Batch Size: " + kafkaBatchSize);
        System.out.println("  Batch delay secs: " + microBatchSeconds);
        System.out.println("Vault Instructions:");
        System.out.println("  AWS Region: " + awsRegion);
        System.out.println("  Secret Name: " + secretName);
        System.out.println("  Vault ID: " + vault_id);
        System.out.println("  Vault URL: " + vault_url);
        System.out.println("  Vault Batch Size: " + vaultBatchSize);
        System.out.println("  Short Circuit Skyflow: " + shortCircuitSkyflow);
        System.out.println("Metrics Instructions:");
        System.out.println("  Namespace: " + cloudwatchNamespace);
        System.out.println("  Reporting Delay Secs: " + reportingDelaySecs);

        if (!localIO) {
            printDiagnosticInfoAndFailFast(awsRegion, kafkaBootstrap, outputBucket, vault_url, shortCircuitSkyflow);
        }

        // Retrieve Skyflow SA credential string from Secrets Manager
        String credentialString = getSecret(awsRegion, secretName);

        // Setup Spark
        Builder sparkBuilder = SparkSession.builder()
                .appName("MSK to Hudi Job")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") // Maybe s3:// would perform better? XXX
        ;
        if (localIO) {
            sparkBuilder = sparkBuilder
                .master("local[1]").config("spark.local.dir", "/tmp/spark-temp") //  Uncomment to run Spark locally
            ;
        }
        SparkSession spark = sparkBuilder.getOrCreate();
        spark.sparkContext().setLogLevel("INFO");

        // Generate schema for given class using Java reflection
        StructType schema = getSchema(clazz);

        // gather output options
        String hudiTablePath = outputBucket + "/tables";
        Map<String,String> hudiOptions = getHudiOptions(clazz, outputBucket, tableName);

        // Build input data frame
        Dataset<Row> inputDF = buildInputDF(spark, localIO, kafkaBootstrap, kafkaTopic, kafkaBatchSize);

        // Repartition for greater parallelism
        //inputDF = inputDF.repartition(numProcessingPartitions);

        // Tokenize sensitive fields using Skyflow.

        MapPartitionsFunction<Row, Row> transformer = new BatchProcessor<>(
            cloudwatchNamespace,
            reportingDelaySecs,
            clazz,
            vaultBatchSize,
            numConcurrentRequests,
            vault_id,
            vault_url,
            credentialString,
            shortCircuitSkyflow
        );
        Dataset<Row> transformedDF = inputDF.mapPartitions(transformer, Encoders.row(schema));

        // Output

        // Let's do a continuous micro-batch every few seconds
        DataStreamWriter<Row> streamWriter = transformedDF
            .writeStream()
            .option("checkpointLocation", outputBucket + "/checkpoints")
            .trigger(Trigger.ProcessingTime(microBatchSeconds + " seconds"))
        ;
        streamWriter = streamWriter
            .format("hudi").outputMode("append")
            .options(hudiOptions)
            .option("path", hudiTablePath)
        ;
        final StreamingQuery query = streamWriter.start();

        // Wait for termination signal
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (query != null && query.isActive()) {
                    query.stop();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        // Run the pipeline!!
        query.awaitTermination();
    }

    private static StructType getSchema(Class<?> clazz) {
        List<StructField> fields = new ArrayList<>();
        fields.add(new StructField("skyflow_id", DataTypes.StringType, false, null));
        Field[] classFields = clazz.getDeclaredFields();
        for (Field field : classFields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String fieldName = field.getName();
                DataType dataType;
                if (field.getType().equals(int.class) || field.getType().equals(Integer.class)) {
                    dataType = DataTypes.IntegerType;
                } else if (field.getType().equals(long.class) || field.getType().equals(Long.class)) {
                    dataType = DataTypes.LongType;
                } else if (field.getType().equals(double.class) || field.getType().equals(Double.class)) {
                    dataType = DataTypes.DoubleType;
                } else if (field.getType().equals(float.class) || field.getType().equals(Float.class)) {
                    dataType = DataTypes.FloatType;
                } else if (field.getType().equals(boolean.class) || field.getType().equals(Boolean.class)) {
                    dataType = DataTypes.BooleanType;
                } else if (field.getType().equals(Date.class)) {
                    dataType = DataTypes.TimestampType;
                } else {
                    dataType = DataTypes.StringType; // catch-all!
                }
                fields.add(DataTypes.createStructField(fieldName, dataType, true));
            }
        }
        fields.sort(Comparator.comparing(field -> field.name()));
        StructType schema = DataTypes.createStructType(fields);
        // System.out.println(fields);
        // System.out.println(schema);
        return schema;
    }

    private static Dataset<Row> buildInputDF(SparkSession spark, boolean localIO, String kafkaBootstrap, String kafkaTopic, int kafkaBatchSize) {
        Map<String, String> kafkaSecurityOptions = new HashMap<>();
        if (localIO) {
            kafkaSecurityOptions.put("kafka.security.protocol", "PLAINTEXT");
        } else {
            kafkaSecurityOptions.put("kafka.security.protocol", "SASL_SSL");
            kafkaSecurityOptions.put("kafka.sasl.mechanism", "AWS_MSK_IAM"); // The following lines are for reading from AWS MSK via IAM; not valid for standalone Kafka
            kafkaSecurityOptions.put("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            kafkaSecurityOptions.put("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler"); // Not sure which one ...
            kafkaSecurityOptions.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler"); // ... does the trick! :) XXX
        }
        // Get the input stream
        /* */
        Dataset<Row> inputDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrap)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", kafkaBatchSize)
            .options(kafkaSecurityOptions)
            .load()
            .selectExpr("CAST(value AS STRING) as valueString")
        ;
        /* */
        /* *
        // For testing WITHOUT sreaming: ceate a static array of strings
        String[] dataArray = {
            "{\"custID\":\"0ff79fbb-9d97-46bd-8ad0-1762bda6a336\",\"firstName\":\"Russell\",\"lastName\":\"Champlin\",\"email\":\"delmer.berge@gmail.com\",\"phoneNumber\":\"(013) 728-8519\",\"dateOfBirth\":\"1998-10-31\",\"addressLine1\":\"66975 Tillman Square\",\"addressLine2\":\"\",\"addressLine3\":\"\",\"city\":\"Bashirianfurt\",\"state\":\"Texas\",\"zip\":\"47279\",\"country\":\"Italy\"}",
            "{\"custID\":\"cd025dfb-b8d8-44a0-a90b-b7bf393ab3e2\",\"firstName\":\"William\",\"lastName\":\"Reichert\",\"email\":\"pierre.purdy@hotmail.com\",\"phoneNumber\":\"(811) 302-0400\",\"dateOfBirth\":\"2002-07-15\",\"addressLine1\":\"7657 Conn Station\",\"addressLine2\":\"\",\"addressLine3\":\"\",\"city\":\"South Caprice\",\"state\":\"Pennsylvania\",\"zip\":\"83315\",\"country\":\"Saint Vincent and the Grenadines\"}",
            "{\"custID\":\"23cf4fdf-b81d-4fa9-a3ae-62f598c6a904\",\"firstName\":\"Janna\",\"lastName\":\"Ebert\",\"email\":\"michal.walker@gmail.com\",\"phoneNumber\":\"616-614-7844 x243\",\"dateOfBirth\":\"1995-05-29\",\"addressLine1\":\"0514 Hammes Dam\",\"addressLine2\":\"\",\"addressLine3\":\"\",\"city\":\"Lake Winfred\",\"state\":\"Nevada\",\"zip\":\"21167-7796\",\"country\":\"Bouvet Island (Bouvetoya)\"}"
        };
        Dataset<Row> inputDF = spark.createDataset(Arrays.asList(dataArray), Encoders.STRING())
                                            .toDF("valueString").withColumn("key", functions.lit(""));
        //kafkaDF.foreach((ForeachFunction<Row>) row -> System.out.println(row.schema() + "  " + row.length() + " " + row.prettyJson()));System.exit(0);
        /* */
        /* *
        // For testing with streaming: read from socket
        Dataset<Row> inputDF = spark
            .readStream()
            .format("org.apache.spark.sql.execution.streaming.TextSocketSourceProvider")
            .option("host", "localhost")
            .option("port", 9999)
            .option("maxOffsetsPerTrigger", kafkaBatchSize)
            .load()
            .selectExpr("CAST(value AS STRING) as valueString");
        /* */

        return inputDF;
    }

    private static Map<String, String> getHudiOptions(Class<? extends SerializableDeserializable> clazz, String outputBucket,
                                      String tableName) {
        String hudiTableName = tableName;

        // Configure Hudi Write
        Map<String, String> hudiOptions = new HashMap<>();
        hudiOptions.put("hoodie.table.name", hudiTableName);
        hudiOptions.put("hoodie.datasource.write.operation", "upsert");
        hudiOptions.put("hoodie.datasource.hive_sync.enable", "false");
        hudiOptions.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE"); // Temp, for Abhishek
        hudiOptions.put("hoodie.datasource.write.hive_style_partitioning", "true");
        hudiOptions.put("hoodie.datasource.write.drop.partition.columns", "true");

        HudiConfig hudiConfigAnn = clazz.getAnnotation(HudiConfig.class);
        if (hudiConfigAnn == null) {
            throw new RuntimeException("HudiConfig annotation is missing on the class: " + clazz.getName());
        }
        hudiOptions.put("hoodie.datasource.write.partitionpath.field", hudiConfigAnn.partitionpathkey_field());
        hudiOptions.put("hoodie.datasource.write.recordkey.field", hudiConfigAnn.recordkey_field());
        hudiOptions.put("hoodie.datasource.write.precombine.field", hudiConfigAnn.precombinekey_field());

        return hudiOptions;
    }

    private static String getSecret(String region, String secretName) {
        SecretsManagerClient client = SecretsManagerClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();

        GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
        return getSecretValueResponse.secretString();
    }

	private static void printDiagnosticInfoAndFailFast(String awsRegion, String kafkaBootstrap, String outputBucket, String vault_url, boolean shortCircuitSkyflow) throws Exception {
        try {
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(awsRegion))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();

            GetCallerIdentityRequest getCallerIdentityRequest = GetCallerIdentityRequest.builder().build();
            GetCallerIdentityResponse getCallerIdentityResponse = stsClient.getCallerIdentity(getCallerIdentityRequest);

            System.out.println("AWS Account: " + getCallerIdentityResponse.account());
            System.out.println("AWS ARN: " + getCallerIdentityResponse.arn());
            System.out.println("AWS UserId: " + getCallerIdentityResponse.userId());
        } catch (Exception e) {
            System.out.println("Failed to get AWS caller identity: " + e.getMessage());
            //allGood = false;
            System.out.println("Ignoring error for now"); // It appears that the EMR master has different IAM role from EMR slave / task. Hence this is only indicative of a few possible problems.
        }

        System.out.println("Current Network settings:"); // Helps make sense of stuff below
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            if (networkInterface.isUp() && !networkInterface.isLoopback()) {
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (inetAddress instanceof Inet4Address) {
                        System.out.println("Interface: " + networkInterface.getDisplayName());
                        System.out.println("IP Address: " + inetAddress.getHostAddress());
                        for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
                            if (interfaceAddress.getAddress().equals(inetAddress)) {
                                System.out.println("Netmask: " + interfaceAddress.getNetworkPrefixLength());
                            }
                        }
                    }
                }
            }
        }
        // Failfast!
        boolean allGood = true;
        // Test the reachablility of the bootstrap servers
        String[] bootstrapServers = kafkaBootstrap.split(",");
        for (String server : bootstrapServers) {
            String[] hostPort = server.split(":");
            String host = hostPort[0];
            int port = Integer.parseInt(hostPort[1]);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 2000);
                System.out.println("Reachable: " + server + " at " + Integer.toString(port));
            } catch (IOException e) {
                // We should probabaly mark not allGood only if we fail to reach all servers
                allGood = false;
                System.out.println("Not reachable: " + server + " at " + Integer.toString(port));
                try {
                    InetAddress inetAddress = InetAddress.getByName(host);
                    System.out.println("    DNS resolved for: " + host + " with IP: " + inetAddress.getHostAddress());
                } catch (UnknownHostException e1) {
                    System.out.println("    DNS not resolved for: " + host);
                }
            }
        }

        // vaultUrl is an https url. check that it is reachable by tcp
        URL url = new URL(vault_url);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(url.getHost(), 443), 2000);
            System.out.println("Reachable: " + vault_url);
        } catch (IOException e) {
            if (!shortCircuitSkyflow) {
                allGood = false;
            }
            System.out.println("Not reachable: " + vault_url);
            String host = url.getHost();
            try {
                InetAddress inetAddress = InetAddress.getByName(host);
                System.out.println("    DNS resolved for: " + host + " with IP: " + inetAddress.getHostAddress());
            } catch (UnknownHostException e2) {
                System.out.println("    DNS not resolved for: " + host);
            }
        }

        if (!allGood) {
            // Message has been printed to stdout
            System.exit(1);
        }
    }
}

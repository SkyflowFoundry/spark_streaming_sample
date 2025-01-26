package com.skyflow.walmartpoc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import com.skyflow.utils.ReflectionUtils;
import com.skyflow.utils.ReflectionUtils.VaultObjectInfo;

public class EmrTask {
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

    public static void main(String[] args) throws Exception {
        System.out.println("Version EMR 5");

        if (args.length < 14) {
            System.err.println("Usage: EmrTask <full.java.class> <output-s3-bucket> <table-name> <kafka-bootstrap> <kafka-topic> <batch-size> <batch-delay-secs> <aws-region> <secret-name> <vault-id> <vault-url> <short-circuit-skyflow?> <namespace> <reporting-delay-secs>");
            System.err.println("Pipeline Type:");
            System.err.println("  <full.java.class>       : The fully qualified Java class name of the object being processed");
            System.err.println("Output Instructions:");
            System.err.println("  <output-s3-bucket>      : The S3 bucket where the output will be stored.");
            System.err.println("  <table-name>            : The name of the table to be used in the process.");
            System.err.println("Input Instructions:");
            System.err.println("  <kafka-bootstrap>       : The Kafka bootstrap server address.");
            System.err.println("  <kafka-topic>           : The Kafka topic to subscribe to.");
            System.err.println("  <batch-size>            : The size of each batch to be processed.");
            System.err.println("  <batch-delay-secs>      : The microbatch size in seconds");
            System.err.println("Vault Instructions:");
            System.err.println("  <aws-region>            : The AWS region where resources are located.");
            System.err.println("  <secret-name>           : The name of the secret in AWS Secrets Manager that stores the vault API key");
            System.err.println("  <vault-id>              : The ID of the vault to be accessed.");
            System.err.println("  <vault-url>             : The URL of the vault service.");
            System.err.println("  <short-circuit-skyflow?>: A boolean flag to determine if Skyflow should be short-circuited.");
            System.err.println("Metrics Instructions:");
            System.err.println("  <namespace>             : The Cloudwatch namespace for the metrics. Empty for no Cloudwatch reporting");
            System.err.println("  <reporting-delay-secs>  : The delay in seconds for reporting metrics.");
            System.exit(1);
        }

        @SuppressWarnings("unchecked")
        Class<? extends SerializableDeserializable> clazz = (Class<? extends SerializableDeserializable>) Class.forName(args[0]);
        String outputBucket = args[1];
        String tableName = args[2];
        String kafkaBootstrap = args[3];
        String kafkaTopic = args[4];
        int batchSize = Integer.parseInt(args[5]);
        int microBatchSeconds = Integer.parseInt(args[6]);
        String awsRegion = args[7];
        String secretName = args[8];
        String vault_id = args[9];
        String vault_url = args[10];
        boolean shortCircuitSkyflow = Boolean.parseBoolean(args[11]);
        String cloudwatchNamespace = args[12];
        int reportingDelaySecs = Integer.parseInt(args[13]);
        // For sanity checking, print out all the gathered args
        System.out.println("Pipeline Type:");
        System.out.println("  Class: " + clazz.getName());
        System.out.println("Output Instructions:");
        System.out.println("  Output Bucket: " + outputBucket);
        System.out.println("  Table Name: " + tableName);
        System.out.println("Input Instructions:");
        System.out.println("  Kafka Bootstrap: " + kafkaBootstrap);
        System.out.println("  Kafka Topic: " + kafkaTopic);
        System.out.println("  Batch Size: " + batchSize);
        System.out.println("  Batch delay secs: " + microBatchSeconds);
        System.out.println("Vault Instructions:");
        System.out.println("  AWS Region: " + awsRegion);
        System.out.println("  Secret Name: " + secretName);
        System.out.println("  Vault ID: " + vault_id);
        System.out.println("  Vault URL: " + vault_url);
        System.out.println("  Short Circuit Skyflow: " + shortCircuitSkyflow);
        System.out.println("Metrics Instructions:");
        System.out.println("  Namespace: " + cloudwatchNamespace);
        System.out.println("  Reporting Delay Secs: " + reportingDelaySecs);

        // Retrieve Skyflow SA credential string from Secrets Manager
        String credentialString = getSecret(awsRegion, secretName);

        // gather output options
        String hudiTablePath = outputBucket + "/tables";
        String hudiTableName = tableName;

        // Configure Hudi Write
        Map<String, String> hudiOptions = new HashMap<>();
        hudiOptions.put("hoodie.table.name", hudiTableName);
        hudiOptions.put("hoodie.datasource.write.operation", "upsert");
        hudiOptions.put("hoodie.datasource.hive_sync.enable", "false");
        hudiOptions.put("hoodie.datasource.write.table.type", "MERGE_ON_READ");

        HudiConfig hudiConfigAnn = clazz.getAnnotation(HudiConfig.class);
        //hudiOptions.put("hoodie.datasource.write.partitionpath.field", hudiConfigAnn.partitionpathkey_field());
        hudiOptions.put("hoodie.datasource.write.recordkey.field", hudiConfigAnn.recordkey_field());
        hudiOptions.put("hoodie.datasource.write.precombine.field", hudiConfigAnn.precombinekey_field());

        //printDiagnosticInfoAndFailFast(awsRegion, kafkaBootstrap, outputBucket, vault_url, shortCircuitSkyflow);

        // Setup Spark
        SparkSession spark = SparkSession.builder()
                .appName("MSK to Hudi Job")
                //.master("local[1]").config("spark.local.dir", "/tmp/spark-temp") //  Uncomment to run Spark locally
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") // Maybe s3:// would perform better? XXX
                .getOrCreate();

        spark.sparkContext().setLogLevel("INFO");

        // Generate schema for given class using Java reflection
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

        // Get the input stream
        /* */
        // For real-use: Read from Kafka
        Dataset<Row> kafkaDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrap)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", batchSize)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "AWS_MSK_IAM") // The following lines are for reading from AWS MSK via IAM; not valid for standalone Kafka
                .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
                .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") // Not sure which one ...
                .option("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler") // ... does the trick! :) XXX
                .load()
                .selectExpr("CAST(value AS STRING) as valueString");
        /* */
        /* *
        // For testing WITHOUT sreaming: ceate a static array of strings
        String[] dataArray = {
            "{\"custID\":\"0ff79fbb-9d97-46bd-8ad0-1762bda6a336\",\"firstName\":\"Russell\",\"lastName\":\"Champlin\",\"email\":\"delmer.berge@gmail.com\",\"phoneNumber\":\"(013) 728-8519\",\"dateOfBirth\":\"1998-10-31\",\"addressLine1\":\"66975 Tillman Square\",\"addressLine2\":\"\",\"addressLine3\":\"\",\"city\":\"Bashirianfurt\",\"state\":\"Texas\",\"zip\":\"47279\",\"country\":\"Italy\"}",
            "{\"custID\":\"cd025dfb-b8d8-44a0-a90b-b7bf393ab3e2\",\"firstName\":\"William\",\"lastName\":\"Reichert\",\"email\":\"pierre.purdy@hotmail.com\",\"phoneNumber\":\"(811) 302-0400\",\"dateOfBirth\":\"2002-07-15\",\"addressLine1\":\"7657 Conn Station\",\"addressLine2\":\"\",\"addressLine3\":\"\",\"city\":\"South Caprice\",\"state\":\"Pennsylvania\",\"zip\":\"83315\",\"country\":\"Saint Vincent and the Grenadines\"}",
            "{\"custID\":\"23cf4fdf-b81d-4fa9-a3ae-62f598c6a904\",\"firstName\":\"Janna\",\"lastName\":\"Ebert\",\"email\":\"michal.walker@gmail.com\",\"phoneNumber\":\"616-614-7844 x243\",\"dateOfBirth\":\"1995-05-29\",\"addressLine1\":\"0514 Hammes Dam\",\"addressLine2\":\"\",\"addressLine3\":\"\",\"city\":\"Lake Winfred\",\"state\":\"Nevada\",\"zip\":\"21167-7796\",\"country\":\"Bouvet Island (Bouvetoya)\"}"
        };
        Dataset<Row> kafkaDF = spark.createDataset(Arrays.asList(dataArray), Encoders.STRING())
                                            .toDF("valueString").withColumn("key", functions.lit(""));
        //kafkaDF.foreach((ForeachFunction<Row>) row -> System.out.println(row.schema() + "  " + row.length() + " " + row.prettyJson()));System.exit(0);
        /* */
        /* *
        // For testing with streaming: read from socket
        Dataset<Row> kafkaDF = spark
                                .readStream()
                                .format("org.apache.spark.sql.execution.streaming.TextSocketSourceProvider")
                                .option("host", "localhost")
                                .option("port", 9999)
                                .option("maxOffsetsPerTrigger", batchSize)
                                .load()
                                .selectExpr("CAST(value AS STRING) as valueString");
        /* */

        // Tokenize sensitive fields using Skyflow.

        MapPartitionsFunction<Row,Row> transformer = new MapPartitionsFunction<Row,Row>() {
            @Override
            @SuppressWarnings("unchecked")
            public Iterator<Row> call(Iterator<Row> iterator) throws Exception {
                int partitionId = TaskContext.getPartitionId();
                System.out.println("Starting partition with ID: " + partitionId);
                try (final CollectorAndReporter stats = new CollectorAndReporter(cloudwatchNamespace, reportingDelaySecs*1000)) {
                    Map<String, String> dimensionMap = new HashMap<>();
                    dimensionMap.put("object", clazz.getSimpleName());
                    ValueDatum numRecords = stats.createOrGetUniqueMetricForName("numRecords", dimensionMap, StandardUnit.COUNT, ValueDatum.class);
                    ValueDatum numBatches = stats.createOrGetUniqueMetricForName("numBatches", dimensionMap, StandardUnit.COUNT, ValueDatum.class);
                    StatisticDatum batchSizes = stats.createOrGetUniqueMetricForName("batchSizes", dimensionMap, StandardUnit.COUNT, StatisticDatum.class);
                    StatisticDatum apiLatency = stats.createOrGetUniqueMetricForName("apiLatency", dimensionMap, StandardUnit.MILLISECONDS, StatisticDatum.class);

                    List<Row> result = new ArrayList<>();
                    while (iterator.hasNext()) {
                        List<String> batch = new ArrayList<>();
                        for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
                            numRecords.increment();
                            batch.add(iterator.next().getString(0)); // There is only one column in the Row: valueString
                        }
                        if (!batch.isEmpty()) {
                            numBatches.increment();
                            batchSizes.value(batch.size());

                            // Process the batch
                            JSONObject[] transformed = getTokenizedObjects(
                                partitionId,
                                apiLatency,
                                batch.toArray(new String[0]),
                                vault_id,
                                vault_url,
                                credentialString,
                                shortCircuitSkyflow,
                                clazz
                            );
                            for (JSONObject obj : transformed) {
                                result.add(RowFactory.create(new TreeMap<>(obj).values().toArray()));
                            }
                        }
                        stats.pollAndReport(false);
                    }
                    System.out.println("Processed " + result.size() + " items in partition with ID: " + partitionId);
                    return result.iterator();
                }
            }
        };

        Dataset<Row> transformedDF = kafkaDF.mapPartitions(transformer, Encoders.row(schema));

        // Output

        // Let's do a continuous micro-batch every few seconds
        StreamingQuery query = transformedDF
                .writeStream()
                .option("checkpointLocation", outputBucket + "/checkpoints")
                .trigger(Trigger.ProcessingTime(microBatchSeconds + " seconds"))
                //.format("console").option("truncate",false).option("numRows",40)
                .format("hudi").outputMode("append")
                .options(hudiOptions)
                .option("path", hudiTablePath)
                .start();

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

    private static <T extends SerializableDeserializable> JSONObject[] getTokenizedObjects(int partitionId, StatisticDatum apiLatency, String[] jsons, String vault_id, String vault_url, String credentialString, boolean shortCircuitSkyflow, Class<T> clazz) throws Exception {
        System.out.println("Processing " + jsons.length + " rows in partition " + partitionId);
        try {
            VaultObjectInfoCache cache = VaultObjectInfoCache.getInstance();
            VaultObjectInfo<T> objectInfo = cache.getVaultObjectInfo(clazz);
            return _getTokenizedObjects(partitionId, apiLatency, jsons, vault_id, vault_url, credentialString, shortCircuitSkyflow, objectInfo, clazz);
        } catch (Exception e) {
            throw new Exception("NOT-FOR-PRODUCTION: Failed to process: " + String.join(", ", jsons), e); // THIS LOGS PII. OBVIOUSLY NOT FOR PRODUCTION USE
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends SerializableDeserializable> JSONObject[] _getTokenizedObjects(int partitionId, StatisticDatum apiLatency, String[] jsons, String vault_id, String vault_url, String credentialString, boolean shortCircuitSkyflow, VaultObjectInfo<T> objectInfo, Class<T> clazz) throws Exception {
        JSONObject[] resultList = new JSONObject[jsons.length];
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

        // Create a JSON/REST request to "vault_url" for inserting records
        String insertRecordUrl = vault_url + "/v1/vaults/" + vault_id + "/" + objectInfo.tableName;
        JSONObject insertReqBody = new JSONObject();
        insertReqBody.put("records", recordsArray);
        insertReqBody.put("tokenization", true);
        insertReqBody.put("upsert", objectInfo.upsertColumnName);

        // Create an HTTP client.
        // XXX create the client in caller and create and cache a connection. Make a request from the cached connection.
        CloseableHttpClient client = HttpClientBuilder.create().build();

        // Create an HTTP request
        ClassicHttpRequest request = ClassicRequestBuilder
            .post(URI.create(insertRecordUrl))
            .addHeader("Authorization", "Bearer " + credentialString)
            .setEntity(insertReqBody.toJSONString(), ContentType.APPLICATION_JSON)
            .build();

        if (!shortCircuitSkyflow) {
            long startTime = System.currentTimeMillis();
            // Send the request and get the response
            @SuppressWarnings("deprecation") // We NEED the synch call
            final CloseableHttpResponse response = client.execute(request);
            if (response.getCode() != 200) {
                String statusLine = response.getReasonPhrase();
                response.close();
                client.close();
                throw new Exception("http response: " + statusLine);
            }

            // Parse the response body as JSON
            JSONParser jsonParser = new JSONParser();
            String bodyString = null;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                bodyString = reader.lines().collect(Collectors.joining("\n"));
            } finally {
                apiLatency.value(System.currentTimeMillis() - startTime);
                response.close();
                client.close();
            }

            JSONObject insertResponse = (JSONObject) jsonParser.parse(bodyString);
            JSONArray responseRecords = (JSONArray) insertResponse.get("records");

            if (responseRecords.size() != jsons.length) {
                throw new RuntimeException("Skyflow was asked to upsert " + jsons.length + " records and returned " + responseRecords.size() + " results");
            }
            for (i = 0; i < responseRecords.size(); i++) {
                JSONObject responseRecord = (JSONObject) responseRecords.get(i);
                JSONObject extractedFields = (JSONObject) responseRecord.get("tokens");
                String skyflow_id = (String) responseRecord.get("skyflow_id");

                T obj = objArray[i];
                ReflectionUtils.replaceWithValuesFromVault(obj, extractedFields, objectInfo);

                String objectJson = obj.toJSONString();
                JSONParser parser = new JSONParser();
                JSONObject jsonObject = (JSONObject) parser.parse(objectJson);
                jsonObject.put("skyflow_id", skyflow_id);
                resultList[i] = jsonObject;
            }
        } else {
            i = 0;
            for (T obj : objArray) {
                //System.out.println(" ("+ partitionId + ") sim send: " + obj);
                String objectJson = obj.toJSONString();
                JSONParser parser = new JSONParser();
                JSONObject jsonObject = (JSONObject) parser.parse(objectJson);
                jsonObject.put("skyflow_id", "skipped");
                resultList[i] =jsonObject;
                i = i + 1;
            }
        }

        return resultList;
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

    @SuppressWarnings("unused")
	private static void printDiagnosticInfoAndFailFast(String awsRegion, String kafkaBootstrap, String outputBucket, String vault_url, boolean shortCircuitSkyflow) throws Exception {
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

        // check that we can write to output bucket, by writing a file ".writablity-test" to it
        String testFileName = "/writability-test";
        String testContent = "This is a test file to check writability.";
        // Use the AWS SDK to write to the S3 bucket
        software.amazon.awssdk.services.s3.S3Client s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
                .region(software.amazon.awssdk.regions.Region.of(awsRegion))
                .build();
        software.amazon.awssdk.services.s3.model.PutObjectRequest putObjectRequest = software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                .bucket(outputBucket)
                .key(testFileName)
                .contentType("text/plain")
                .build();
        try {
            s3Client.putObject(putObjectRequest, software.amazon.awssdk.core.sync.RequestBody.fromString(testContent));
            System.out.println("Successfully wrote test file to output bucket: " + outputBucket);
        } catch (S3Exception e3) {
            System.out.println("Can't write to bucket: " + outputBucket + " : File : " + testFileName + " : " + e3.awsErrorDetails().errorMessage());
            e3.printStackTrace();
            //allGood = false;
            System.out.println("Ignoring error for now"); // It appears that the EMR master has different IAM role from EMR slave / task. Hence this is only indicative of a few possible problems.
        }

        if (!allGood) {
            // Message has been printed to stdout
            System.exit(1);
        }
    }
}

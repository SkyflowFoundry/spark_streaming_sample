package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;
import com.google.common.util.concurrent.RateLimiter;

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;


public class KafkaCustomerPublisher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCustomerPublisher.class);

    final KafkaProducer<String, String> producer;
    final String topic;
    final Faker faker;
    final List<CountryZipCityState> czcs;
    final CollectorAndReporter stats;

    KafkaCustomerPublisher(String brokerServer, String topic,
                           Faker faker, List<CountryZipCityState> czcs,
                           CollectorAndReporter stats) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServer);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // AWS MSK specific config
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        this.topic = topic;
        this.faker = faker;
        this.czcs = czcs;
        this.stats = stats;

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    public void send(Customer customer, Callback callback) throws IOException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, customer.toJSONString());
        producer.send(record, callback);
    }

    public void produce_batch_at_rate(long numRecs, double numPerSecond) throws IOException {
        ValueDatum numRecords = stats.createOrGetUniqueMetricForName("numRecordsPublished", null, StandardUnit.COUNT, ValueDatum.class);
        ValueDatum numErrors = stats.createOrGetUniqueMetricForName("numPublishErrors", null, StandardUnit.COUNT, ValueDatum.class);

        RateLimiter throttler = RateLimiter.create(numPerSecond);

        long startTime = System.currentTimeMillis();
        long lastWarningTime = startTime; // ignore rates for the first few seconds
        for (long i = 0; i < numRecs; i++) {
            throttler.acquire();

            Customer customer = new Customer(faker, czcs);
            //logger.debug("sending message...");
            send(customer, (metadata, exception) -> {
                if (exception != null) {
                    numErrors.increment();
                    logger.error("Message sending failed: {}", exception.getMessage(), exception);
                } else {
                    numRecords.increment();
                    logger.debug("Message sent successfully to topic {} partition {} with offset {}", metadata.topic(), metadata.partition(), metadata.offset());
                }
                stats.pollAndReport(false);
            });

            // Check if we are falling behind the desired rate limit.
            // Do not check if we just printed a warning less than 30s ago. Also ignore if we are within the first 1000 records.
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastWarningTime >= 30000 && i>=1000) {
                long elapsedTime = currentTime - startTime;
                double expectedTime = (i + 1) / numPerSecond * 1000;
                if (elapsedTime > expectedTime * 1.1) {
                    logger.warn("Falling more than 10% behind the desired rate limit. Num: {}. Desired rate: {} records/sec, Current rate: {} records/sec", i, numPerSecond, (i + 1) / (elapsedTime / 1000.0));
                    lastWarningTime = currentTime;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: " + KafkaCustomerPublisher.class.getName() + " <num-recs> <num-per-second> <kafka-bootstrap> <kafka-topic> <namespace> <reporting-delay-secs>");
            System.err.println("Generation Instructions:");
            System.err.println("  <num-recs>              : The number of records to generate.");
            System.err.println("  <num-per-second>        : The number of records to generate per second.");
            System.err.println("Output Instructions:");
            System.err.println("  <kafka-bootstrap>       : The Kafka bootstrap server address.");
            System.err.println("  <kafka-topic>           : The Kafka topic to subscribe to.");
            System.err.println("Metrics Instructions:");
            System.err.println("  <namespace>             : The Cloudwatch namespace for the metrics. Empty for no Cloudwatch reporting");
            System.err.println("  <reporting-delay-secs>  : The delay in seconds for reporting metrics.");
            System.exit(1);
        }
        
        long numRecs = Long.parseLong(args[0]);
        double numPerSecond = Double.parseDouble(args[1]);
        String brokerServer = args[2];
        String topicName = args[3];
        String cloudwatchNamespace = args[4];
        int reportingDelaySecs = Integer.parseInt(args[5]);

        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.log.org.slf4j.MDC", "instanceId");

        logger.info("Broker: " + brokerServer);
        logger.info("Topic: " + topicName);

        // Initialize Faker & supporting stuff
        List<CountryZipCityState> czcs = CountryZipCityState.loadData("US.tsv");
        Faker faker = new Faker();

        try (final CollectorAndReporter stats = new CollectorAndReporter(cloudwatchNamespace, reportingDelaySecs*1000);
             final KafkaCustomerPublisher producer = new KafkaCustomerPublisher(brokerServer, topicName, faker, czcs, stats);) {
                producer.produce_batch_at_rate(numRecs, numPerSecond);
        }
        logger.info("Done.");
    }
}

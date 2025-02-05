package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;


public class KafkaCustomerPublisher {
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

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Usage: " + KafkaCustomerPublisher.class.getName() + " <load-shape> <kafka-bootstrap> <kafka-topic> <kafka-partitions> <namespace> <reporting-delay-secs>");
            System.err.println("Generation Instructions:");
            System.err.println("  <load-shape>            : Semicolon separated list of rate,mins pairs: ");
            System.err.println("                              generate 'rate' (double) records for 'mins' (int) minutes");
            System.err.println("Output Instructions:");
            System.err.println("  <kafka-bootstrap>       : The Kafka bootstrap server address.");
            System.err.println("  <kafka-topic>           : The Kafka topic to subscribe to.");
            System.err.println("  <kafka-partitions>      : Comma separated partition numbers to round-robin to.");
            System.err.println("                              Can be empty string, meaning all partitions");
            System.err.println("Metrics Instructions:");
            System.err.println("  <namespace>             : The Cloudwatch namespace for the metrics. Empty for no Cloudwatch reporting");
            System.err.println("  <reporting-delay-secs>  : The delay in seconds for reporting metrics.");
            System.exit(1);
        }

        String loadShape = args[0];
        String brokerServer = args[1];
        String topicName = args[2];
        int[] kafkaPartitions = null;
        if (!args[3].isEmpty()) {
            String[] partitionStrings = args[3].split(",");
            List<Integer> partitionList = new ArrayList<>();
            for (String partitionString : partitionStrings) {
                partitionString = partitionString.trim();
                if (partitionString.contains("-")) {
                    String[] range = partitionString.split("-");
                    int start = Integer.parseInt(range[0].trim());
                    int end = Integer.parseInt(range[1].trim());
                    for (int j = start; j <= end; j++) {
                        partitionList.add(j);
                    }
                } else {
                    partitionList.add(Integer.parseInt(partitionString));
                }
            }
            kafkaPartitions = partitionList.stream().mapToInt(Integer::intValue).toArray();
        }
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

        try (CollectorAndReporter stats = new CollectorAndReporter(cloudwatchNamespace, reportingDelaySecs*1000);
                KafkaPublisher<Customer> customerPublisher = new KafkaPublisher<>(Customer.class, false, brokerServer, topicName, kafkaPartitions, stats);) {
            long totalItems = LoadRunner.run(loadShape, new Runnable() {
                @Override
                public void run() {
                    Customer customer = new Customer(faker, czcs);
                    try {
                        customerPublisher.publish(customer);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }                
            });
            logger.info("Created {} Customers",totalItems);
        }
    }
}

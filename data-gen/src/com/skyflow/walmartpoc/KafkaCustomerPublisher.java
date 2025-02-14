package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;


public class KafkaCustomerPublisher {
    private static final Logger logger = LoggerFactory.getLogger(KafkaCustomerPublisher.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            System.err.println("Usage: " + KafkaCustomerPublisher.class.getName() + " <run-locally> <load-shape> <kafka-bootstrap> <kafka-topic> <kafka-partitions> <namespace> <reporting-delay-secs>");
            System.err.println("Generation Instructions:");
            System.err.println("  <run-locally>           : Run locally (true/false).");
            System.err.println("  <load-shape>            : Semicolon separated list of rate,mins pairs: ");
            System.err.println("                              generate 'rate' (double) records for 'mins' (int) minutes");
            System.err.println("Output Instructions:");
            System.err.println("  <kafka-bootstrap>       : The Kafka bootstrap server address.");
            System.err.println("  <kafka-topic-base>      : The base of the Kafka topic to subscribe to.");
            System.err.println("  <kafka-partitions>      : Comma separated partition numbers to round-robin to.");
            System.err.println("                              Can be empty string, meaning all partitions");
            System.err.println("Metrics Instructions:");
            System.err.println("  <namespace>             : The Cloudwatch namespace for the metrics. Empty for no Cloudwatch reporting");
            System.err.println("  <reporting-delay-secs>  : The delay in seconds for reporting metrics.");
            System.exit(1);
        }

        boolean runLocally = Boolean.parseBoolean(args[0]);
        String loadShape = args[1];
        String brokerServer = args[2];
        String topicBase = args[3];
        String kafkaPartitions = args[4];
        String cloudwatchNamespace = args[5];
        int reportingDelaySecs = Integer.parseInt(args[6]);

        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.log.org.slf4j.MDC", "instanceId");
        logger.info("Broker: " + brokerServer);
        logger.info("Topic: " + topicBase);

        LoadRunner.terminateOnInterruption();

        // Initialize Faker & supporting stuff
        List<CountryZipCityState> czcs = CountryZipCityState.loadData("US.tsv");
        Faker faker = new Faker();

        try (CollectorAndReporter stats = new CollectorAndReporter(cloudwatchNamespace, reportingDelaySecs*1000);
                KafkaPublisher<Customer> customerPublisher = new KafkaPublisher<>(Customer.class, runLocally, brokerServer, topicBase, kafkaPartitions, stats);) {
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

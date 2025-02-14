package com.skyflow.walmartpoc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;

public class GenCatalog {
    private static final Logger logger = LoggerFactory.getLogger(GenCatalog.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            System.err.println("Usage: java " + GenCatalog.class.getName() + " <runLocally> <configFilePath> <load-shape> <outputDir> <kafka-bootstrap> <kafka-topic-base> <kafka-partitions> <namespace> <reporting-delay-secs>");
            System.err.println("Generation Instructions:");
            System.err.println("  <runLocally>            : Run locally (write to plaintext Kafka)");
            System.err.println("  <configFilePath>        : Path to config.yml file ");
            System.err.println("  <load-shape>            : Semicolon separated list of rate,mins pairs: ");
            System.err.println("                              generate 'rate' (double) records for 'mins' (int) minutes");
            System.err.println("  <outputDir>             : The dir where all the local file I/O happens ");
            System.err.println("Stream Output Instructions:");
            System.err.println("  <kafka-bootstrap>       : The Kafka bootstrap server address.");
            System.err.println("  <kafka-topic-base>      : The base name of the Kafka topic to write to.");
            System.err.println("  <kafka-partitions>      : Comma separated partition numbers to round-robin to.");
            System.err.println("                              Can be empty string, meaning all partitions");
            System.err.println("Metrics Instructions:");
            System.err.println("  <namespace>             : The Cloudwatch namespace for the metrics. Empty for no Cloudwatch reporting");
            System.err.println("  <reporting-delay-secs>  : The delay in seconds for reporting metrics.");
            System.exit(1);
        }
        boolean runLocally = Boolean.parseBoolean(args[0]);
        String configFilePath = args[1];
        String loadShape = args[2];
        String outputDir = args[3];
        String kafkaBootstrap = args[4];
        String kafkaTopicBase = args[5];
        String kafkaPartitions = args[6];
        String namespace = args[7];
        int reportingDelaySecs = Integer.parseInt(args[8]);

        Config config = Config.load(configFilePath);

        // Ensure output directory exists
        Files.createDirectories(Paths.get(outputDir));

        Faker faker = new Faker();

        try (CollectorAndReporter stats = new CollectorAndReporter(namespace, reportingDelaySecs*1000);
                KafkaPublisher<Catalog> catalogPublisher = new KafkaPublisher<>(Catalog.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);
                CsvWriter<Catalog> catalogWriter = new CsvWriter<>(Paths.get(outputDir, config.seed_data.catalog_file), Catalog.getCsvHeader());) {
            long totalItems = LoadRunner.run(loadShape, new Runnable() {
                @Override
                public void run() {
                    Catalog item = new Catalog(faker);
                    catalogWriter.write(item);
                    try {
                        catalogPublisher.publish(item);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }                
            });
            logger.info("Created {} Catalog items",totalItems);
        }
    }
}
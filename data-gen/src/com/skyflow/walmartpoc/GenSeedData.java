package com.skyflow.walmartpoc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;

public class GenSeedData {
    private static final Logger logger = LoggerFactory.getLogger(GenSeedData.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 9) {
            System.err.println("Usage: java " + GenSeedData.class.getName() + " <runLocally> <configFilePath> <load-shape> <outputDir> <kafka-bootstrap> <kafka-topic-base> <kafka-partitions> <namespace> <reporting-delay-secs>");
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
        int[] kafkaPartitions = null;
        if (!args[6].isEmpty()) {
            String[] partitionStrings = args[6].split(",");
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
        String namespace = args[7];
        int reportingDelaySecs = Integer.parseInt(args[8]);

        Config config = Config.load(configFilePath);

        // Ensure output directory exists
        Files.createDirectories(Paths.get(outputDir));

        // Read Catalog
        CsvReader<Catalog> catalogReader = new CsvReader<>(Catalog.class, Paths.get(outputDir, config.seed_data.catalog_file), Catalog.getCsvHeader());
        List<Catalog> x = new ArrayList<>();
        for (Catalog c : catalogReader) {
            x.add(c);
        }
        Catalog[] catalog = x.toArray(new Catalog[0]);

        // Initialize Faker & supporting stuff
        Random random = new Random();
        List<CountryZipCityState> czcs = CountryZipCityState.loadData("US.tsv");
        Faker faker = new Faker();
        
        float one_card_fraction = config.fake_data.one_card_fraction;
        float two_card_fraction = config.fake_data.two_card_fraction;
        float zeroTxnProb = config.fake_data.zero_txn_fraction;
        float twoTxnProb = config.fake_data.two_txn_fraction;
        float randomValue = random.nextFloat();

        try (CollectorAndReporter stats = new CollectorAndReporter(namespace, reportingDelaySecs*1000);
             CsvWriter<Customer> customerWriter = new CsvWriter<>(Paths.get(outputDir, config.seed_data.customers_file), Customer.getCsvHeader());
             CsvWriter<PaymentInfo> paymentWriter = new CsvWriter<>(Paths.get(outputDir, config.seed_data.payments_file), PaymentInfo.getCsvHeader());
             CsvWriter<Transaction> transactionWriter = new CsvWriter<>(Paths.get(outputDir, config.seed_data.transactions_file), Transaction.getCsvHeader());
             CsvWriter<ConsentPreference>  consentWriter = new CsvWriter<>(Paths.get(outputDir, config.seed_data.consent_file), ConsentPreference.getCsvHeader());
             KafkaPublisher<Customer> customerPublisher = new KafkaPublisher<>(Customer.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);
             KafkaPublisher<PaymentInfo> paymentPublisher = new KafkaPublisher<>(PaymentInfo.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);
             KafkaPublisher<Transaction> transactionPublisher = new KafkaPublisher<>(Transaction.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);
             KafkaPublisher<ConsentPreference> consentPublisher = new KafkaPublisher<>(ConsentPreference.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);) {
            long totalItems = LoadRunner.run(loadShape, new Runnable() {
                @Override
                public void run() {
                    try {
                        // Generate Customer Data
                        Customer customer = new Customer(faker, czcs);
                        // Write Customer Data to CSV
                        customerWriter.write(customer);
                        customerPublisher.publish(customer);
                        logger.info("Wrote customer: {}",customer);

                        // Determine number of payment cards
                        int paymentCardCount;
                        int percentage = random.nextInt(100) + 1; // 1 to 100
                        if (percentage <= 100*one_card_fraction) {
                            paymentCardCount = 1;
                        } else if (percentage <= 100*(one_card_fraction+two_card_fraction)) {
                            paymentCardCount = 2;
                        } else {
                            paymentCardCount = 3;
                        }

                        for (int j = 0; j < paymentCardCount; j++) {
                            // Generate Payment Card Data
                            PaymentInfo paymentInfo = new PaymentInfo(customer, faker);

                            // Write Payment Card Data to CSV
                            paymentWriter.write(paymentInfo);
                            paymentPublisher.publish(paymentInfo);
                            logger.info("Wrote PaymentInfo: {}",paymentInfo);
                        }

                        int transactionCount;
                        if (randomValue < zeroTxnProb) {
                            transactionCount = 0;
                        } else if (randomValue < zeroTxnProb + twoTxnProb) {
                            transactionCount = 2;
                        } else {
                            transactionCount = 10;
                        }
                
                        for (int i = 0; i < transactionCount; i++) {
                            Transaction transaction = new Transaction(customer, catalog[random.nextInt(catalog.length)], faker);
                
                            // Write Transaction Data to CSV
                            transactionWriter.write(transaction);
                            transactionPublisher.publish(transaction);
                            logger.info("Wrote Transaction {}",transaction);
                        }

                        ConsentPreference preference = new ConsentPreference("custId",customer.custID,"otherId",faker.internet().uuid(),faker);
                        consentWriter.write(preference);
                        consentPublisher.publish(preference);
                        logger.info("Wrote Consentpref {}",preference);
                    } catch (IOException e) {
                        throw new RuntimeException("An error occurred while generating data", e);
                    }
                }
            });
            logger.info("Iterated {} times",totalItems);
        }
        catalogReader.close();
    }
}


package com.skyflow.walmartpoc;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.opencsv.exceptions.CsvValidationException;

public class UpsertGenerator {
    private static final Logger logger = LoggerFactory.getLogger(UpsertGenerator.class);

    private final Catalog[] seeded_catalog;
    private final Customer[] seeded_customers;
    private final PaymentInfo[] seeded_payments;
    private final Faker faker = new Faker();
    private final Random random = new Random();
    private final List<CountryZipCityState> czcs;

    // the probabilities for the upsert operation
    private final float insert_probability;
    private final float street_address_change_probability;
    private final float email_change_probability;
    private final float mhmd_yes_fraction;

    public UpsertGenerator(Config config, String outputDir, CollectorAndReporter stats) throws IOException, CsvValidationException {
        insert_probability = config.upsert_probabilities.insert_probability;
        email_change_probability = config.upsert_probabilities.email_change_probability;
        street_address_change_probability = config.upsert_probabilities.street_address_change_probability;
        mhmd_yes_fraction = config.fake_data.mhmd_yes_fraction;

        czcs = CountryZipCityState.loadData("US.tsv");

        seeded_catalog = CsvReader.readCsvFile(Catalog.class, Paths.get(outputDir, config.seed_data.catalog_file), Catalog.getCsvHeader());
        logger.info("Read {} Catalog items", seeded_catalog.length);
        seeded_customers = CsvReader.readCsvFile(Customer.class, Paths.get(outputDir, config.seed_data.customers_file), Customer.getCsvHeader());
        logger.info("Read {} Customer items", seeded_customers.length);
        seeded_payments = CsvReader.readCsvFile(PaymentInfo.class, Paths.get(outputDir, config.seed_data.payments_file), PaymentInfo.getCsvHeader());
        logger.info("Read {} PaymentInfo",seeded_payments.length);
    }

    public Customer getCustomer() {
        // Determine if we should create a new customer or choose an existing one
        float r = random.nextFloat();
        Customer customer;
        if (r < insert_probability) {
            // Create a new customer
            customer = new Customer(faker, czcs);
        } else {
            // Choose an existing customer at random
            int randomIndex = random.nextInt(seeded_customers.length);
            customer = seeded_customers[randomIndex];

            r = random.nextFloat();
            // Determine if we should change the street address or email based on probabilities
            if (r < street_address_change_probability) {
                // Change street address
                Address address = faker.address();
                customer.addressLine1 = address.streetAddress();
                customer.addressLine2 = address.secondaryAddress();
            } else if (r < street_address_change_probability + email_change_probability) {
                // Change email
                customer.email = faker.internet().emailAddress();
            } else {
                // no change!
            }
            customer.lastupdate_ts = System.currentTimeMillis();
        }

        return customer;
    }

    public PaymentInfo getPaymentInfo() {
        // Determine if we should create a new payment info or choose an existing one
        float r = random.nextFloat();
        PaymentInfo paymentInfo;
        if (r < insert_probability) {
            // Choose a random customer to add this payment info to
            int randomIndex = random.nextInt(seeded_customers.length);
            Customer randomCustomer = seeded_customers[randomIndex];
            paymentInfo = new PaymentInfo(randomCustomer, faker);
        } else {
            // Choose an existing payment info at random
            int randomIndex = random.nextInt(seeded_payments.length);
            paymentInfo = seeded_payments[randomIndex];

            r = random.nextFloat();
            // Determine if we should change the street address or email based on probabilities
            if (r < street_address_change_probability) {
                // Change street address
                Address address = faker.address();
                paymentInfo.cardHolderAddressLine1 = address.streetAddress();
                paymentInfo.cardHolderAddressLine2 = address.secondaryAddress();
            } else if (r < street_address_change_probability + email_change_probability) {
                // Change email
                paymentInfo.cardHolderEmail = faker.internet().emailAddress();
            } else {
                // no change!
            }
            paymentInfo.lastupdate_ts = System.currentTimeMillis();
        }

        return paymentInfo;
    }

    public Transaction getNewTransaction() {
        // Choose a random customer for the transaction
        int randomCustomerIndex = random.nextInt(seeded_customers.length);
        Customer customer = seeded_customers[randomCustomerIndex];
        // Choose a random catalog item
        int randomCatalogIndex = random.nextInt(seeded_catalog.length);
        Catalog catalogItem = seeded_catalog[randomCatalogIndex];
        Transaction transaction = new Transaction(customer, catalogItem, faker, mhmd_yes_fraction);
        
        return transaction;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            System.err.println("Usage: java " + UpsertGenerator.class.getName() + " <runLocally> <configFilePath> <load-shape> <outputDir> <kafka-bootstrap> <kafka-topic-base> <kafka-partitions> <namespace> <reporting-delay-secs>");
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

        //System.out.println("Args gathered");
        Config config = Config.load(configFilePath);

        //System.out.println("Starting stats & publishers");
        try (CollectorAndReporter stats = new CollectorAndReporter(namespace, reportingDelaySecs*1000);
             KafkaPublisher<Customer> customerPublisher = new KafkaPublisher<>(Customer.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);
             KafkaPublisher<PaymentInfo> paymentPublisher = new KafkaPublisher<>(PaymentInfo.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);
             KafkaPublisher<Transaction> transactionPublisher = new KafkaPublisher<>(Transaction.class, runLocally, kafkaBootstrap, kafkaTopicBase, kafkaPartitions, stats);) {

            //System.out.println("Starting generator");
            UpsertGenerator generator = new UpsertGenerator(config, outputDir, stats);

            //System.out.println("Starting load");
            long totalItems = LoadRunner.run(loadShape, new Runnable() {
                @Override
                public void run() {
                    try {
                        customerPublisher.publish(generator.getCustomer());
                        paymentPublisher.publish(generator.getPaymentInfo());
                        transactionPublisher.publish(generator.getNewTransaction());
                    } catch (IOException e) {
                        throw new RuntimeException("An error occurred while generating data", e);
                    }
                }
            });
            logger.info("Iterated {} times",totalItems);
        }     
    }
}
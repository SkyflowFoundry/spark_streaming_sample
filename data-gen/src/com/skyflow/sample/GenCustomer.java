package com.skyflow.sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;


public class GenCustomer {
    private static final Logger logger = LoggerFactory.getLogger(GenCustomer.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 10) {
            System.err.println("Usage: " + GenCustomer.class.getName() + " "
                + "<config-file> "
                + "<generation-load-shape> " 
                + "<upsert-load-shape> "
                + "<num-concurrent-requests> "
                + "<aws-region> "
                + "<secret-name> "
                + "<vault-id> "
                + "<vault-url> "
                + "<namespace> " 
                + "<reporting-delay-secs> "
            );
            System.err.println("Configuration Instructions:");
            System.err.println("  <config-file>           : Path to the configuration file.");
            System.err.println("Generation Instructions:");
            System.err.println("  <generation-load-shape> : Semicolon separated list of rate,mins pairs: ");
            System.err.println("                              generate 'rate' (double) records for 'mins' (int) minutes");
            System.err.println("  <upsert-load-shape>     : Semicolon separated list of rate,mins pairs: ");
            System.err.println("                              upsert 'rate' (double) records for 'mins' (int) minutes");
            System.err.println("  <num-concurrent-requests>: The number of concurrent API requests.");
            System.err.println("Vault Instructions:");
            System.err.println("  <aws-region>            : The AWS region where resources are located.");
            System.err.println("  <secret-name>           : The name of the secret in AWS Secrets Manager that stores the vault API key");
            System.err.println("  <vault-id>              : The ID of the vault to be accessed.");
            System.err.println("  <vault-url>             : The URL of the vault service.");
            System.err.println("Metrics Instructions:");
            System.err.println("  <namespace>             : The Cloudwatch namespace for the metrics. Empty for no Cloudwatch reporting");
            System.err.println("  <reporting-delay-secs>  : The delay in seconds for reporting metrics.");
            System.exit(1);
        }

        String configFile = args[0];
        String generationLoadShape = args[1];
        String upsertLoadShape = args[2];
        int numConcurrentRequests = Integer.parseInt(args[3]);
        String awsRegion = args[4];
        String secretName = args[5];
        String vault_id = args[6];
        String vault_url = args[7];
        String cloudwatchNamespace = args[8];
        int reportingDelaySecs = Integer.parseInt(args[9]);

        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.log.org.slf4j.MDC", "instanceId");

        logger.info("Generation Instructions:");
        logger.info("  Generation Load Shape: {}", generationLoadShape);
        logger.info("  Upsert Load Shape: {}", upsertLoadShape);
        logger.info("  Num Concurrent Requests: {}", numConcurrentRequests);
        logger.info("Vault Instructions:");
        logger.info("  AWS Region: {}", awsRegion);
        logger.info("  Secret Name: {}", secretName);
        logger.info("  Vault ID: {}", vault_id);
        logger.info("  Vault URL: {}", vault_url);
        logger.info("Metrics Instructions:");
        logger.info("  Namespace: {}", cloudwatchNamespace);
        logger.info("  Reporting Delay Secs: {}", reportingDelaySecs);

        int insertPhaseIterations = LoadRunner.getNumber(generationLoadShape);

        Config config = Config.load(configFile);
        float insert_probability = config.upsert_probabilities.insert_probability;
        float email_change_probability = config.upsert_probabilities.email_change_probability;
        float street_address_change_probability = config.upsert_probabilities.street_address_change_probability;

        String apiKey = getSecret(awsRegion, secretName);

        LoadRunner.terminateOnInterruption();
        
        // Initialize Faker & supporting stuff
        List<CountryZipCityState> czcs = CountryZipCityState.loadData("US.tsv");
        Faker faker = new Faker();

        try (CollectorAndReporter stats = new CollectorAndReporter(cloudwatchNamespace, reportingDelaySecs*1000);
             VaultDataLoader<Customer> loader = new VaultDataLoader<>(vault_id, vault_url, numConcurrentRequests, apiKey, stats, Customer.class);) {
            ValueDatum inserts = stats.createOrGetUniqueMetric("inserts", null, StandardUnit.COUNT, ValueDatum.class);
            ValueDatum updates_0 = stats.createOrGetUniqueMetric("updates", new HashMap<String,String>(){{put("change","no");}}, StandardUnit.COUNT, ValueDatum.class);
            ValueDatum updates_1 = stats.createOrGetUniqueMetric("updates", new HashMap<String,String>(){{put("change","1");}}, StandardUnit.COUNT, ValueDatum.class);
            ValueDatum updates_2 = stats.createOrGetUniqueMetric("updates", new HashMap<String,String>(){{put("change","2");}}, StandardUnit.COUNT, ValueDatum.class);

            List<Customer> customers =  new ArrayList<>(insertPhaseIterations);
            Random random = new Random();

            {
                long totalItems = LoadRunner.run(generationLoadShape, new Runnable() {
                    @Override
                    public void run() {
                        Customer customer = new Customer(faker, czcs);
                        customers.add(customer);
                        loader.loadObjectIntoVault(customer, true);
                        inserts.increment();
                        stats.pollAndReport(false);
                    }                
                });
                logger.info("Created {} Customers",totalItems);
            }

            {
                int numCustomers = customers.size();
                long totalItems = LoadRunner.run(upsertLoadShape, new Runnable() {
                    @Override
                    public void run() {
                        // Determine if we should create a new customer or choose an existing one
                        float r = random.nextFloat();
                        Customer customer;
                        final ValueDatum datum;
                        if (r < insert_probability) {
                            // Create a new customer
                            customer = new Customer(faker, czcs);
                            datum = inserts;
                        } else {
                            // Choose an existing customer at random
                            int randomIndex = random.nextInt(numCustomers);
                            customer = customers.get(randomIndex);

                            r = random.nextFloat();
                            // Determine if we should change the street address or email based on probabilities
                            if (r < street_address_change_probability) {
                                // Change street address
                                Address address = faker.address();
                                customer.addressLine1 = address.streetAddress();
                                customer.addressLine2 = address.secondaryAddress();
                                datum = updates_2;
                            } else if (r < street_address_change_probability + email_change_probability) {
                                // Change email
                                customer.email = faker.internet().emailAddress();
                                datum = updates_1;
                            } else {
                                // no change!
                                datum = updates_0;
                            }
                            customer.lastupdate_ts = System.currentTimeMillis();
                        }
                        loader.loadObjectIntoVault(customer, true);
                        datum.increment();
                        stats.pollAndReport(false);
                    }                
                });
                logger.info("Upserted {} Customers",totalItems);
            }
        }
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
}

package com.skyflow.walmartpoc;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

public class GenSeedData {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: java GenSeedData <configFilePath> <outputDir>");
            System.exit(1);
        }
        String configFilePath = args[0];
        String outputDir = args[1];

        Config config = Config.load(configFilePath);

        try (CSVWriter customerWriter = new CSVWriter(new FileWriter(Paths.get(outputDir, config.seed_data.customers_file).toString()));
             CSVWriter paymentWriter = new CSVWriter(new FileWriter(Paths.get(outputDir,config.seed_data.payments_file).toString()));
             CSVWriter tokenizedcustomerWriter = new CSVWriter(new FileWriter(Paths.get(outputDir, config.seed_data.tokenized_customers_file).toString()));
             CSVWriter tokenizedpaymentWriter = new CSVWriter(new FileWriter(Paths.get(outputDir,config.seed_data.tokenized_payments_file).toString()))) {
                gen_and_write_seeded_data(config, outputDir, customerWriter, paymentWriter, tokenizedcustomerWriter, tokenizedpaymentWriter);
                System.out.println("Data generation completed.");
             }
    }

    static void gen_and_write_seeded_data(Config config, String outputDir,
                CSVWriter customerWriter, CSVWriter paymentWriter,
                CSVWriter tokenizedCustomerWriter, CSVWriter tokenizedPaymentWriter
                ) throws Exception {

        int customerCount = config.seed_data.total_customer_count;
        float one_card_fraction = config.fake_data.one_card_fraction;
        float two_card_fraction = config.fake_data.two_card_fraction;

        // Ensure output directory exists
        Files.createDirectories(Paths.get(outputDir));

        // Initialize Faker
        Faker faker = new Faker();

        // If required, initialize vault loader
        VaultDataLoader<Customer> customer_loader = null;
        VaultDataLoader<PaymentInfo> payments_loader = null;
        TokenProvider tokenProvider = new TokenProvider(new String(Files.readAllBytes(Paths.get(config.vault.private_key_file))));
        if (config.seed_data.load_to_vault) {
            customer_loader = new VaultDataLoader<>(config.vault.vault_id, config.vault.vault_url, config.vault.max_rows_in_batch,
                                                    tokenProvider, Customer.class);
            payments_loader = new VaultDataLoader<>(config.vault.vault_id, config.vault.vault_url, config.vault.max_rows_in_batch,
                                                    tokenProvider, PaymentInfo.class);
        }

        // Write headers
        customerWriter.writeNext(Customer.CUSTOMER_CSV_HEADER);
        paymentWriter.writeNext(PaymentInfo.PAYMENT_CSV_HEADER);
        tokenizedCustomerWriter.writeNext(new String[]{"skyflow_id","CustID", "FirstName", "LastName", "Email", "PhoneNumber", "DateOfBirth", "AddressLine1", "AddressLine2", "AddressLine3", "City", "State", "Zip", "Country"});
        tokenizedPaymentWriter.writeNext(new String[]{"skyflow_id","PaymentID", "CustID", "CreditCardNumber", "CardExpiry", "CardCVV", "CardHolderFirstName", "CardHolderLastName", "CardHolderPhoneNumber", "CardHolderAddressLine1", "CardHolderAddressLine2", "CardHolderAddressLine3", "CardHolderCity", "CardHolderState", "CardHolderZip", "CardHolderCountry", "CardHolderEmail", "CardHolderDateOfBirth"});

        Random random = new Random();

        for (int i = 0; i < customerCount; i++) {
            // Generate Customer Data
            Customer customer = new Customer(faker);

            // Write Customer Data to CSV
            String[] customerData = {
                customer.custID, customer.firstName, customer.lastName, customer.email, customer.phoneNumber, customer.dateOfBirth, 
                customer.addressLine1, customer.addressLine2, customer.addressLine3, customer.city, customer.state, customer.zip, customer.country
            };
            customerWriter.writeNext(customerData, true);

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
                String[] paymentData = {
                    paymentInfo.paymentID, paymentInfo.custID, paymentInfo.creditCardNumber, paymentInfo.cardExpiry, paymentInfo.cardCVV,
                    paymentInfo.cardHolderFirstName, paymentInfo.cardHolderLastName, paymentInfo.cardHolderPhoneNumber,
                    paymentInfo.cardHolderAddressLine1, paymentInfo.cardHolderAddressLine2, paymentInfo.cardHolderAddressLine3,
                    paymentInfo.cardHolderCity, paymentInfo.cardHolderState, paymentInfo.cardHolderZip, paymentInfo.cardHolderCountry,
                    paymentInfo.cardHolderEmail, paymentInfo.cardHolderDateOfBirth
                };

                // Write Payment Card Data to CSV
                paymentWriter.writeNext(paymentData, true);

                // Load data into the vault
                if (payments_loader!=null) {
                    String payment_skyflow_id = payments_loader.loadObjectIntoVault(paymentInfo);
                    String[] tokenizedPaymentData = {
                        payment_skyflow_id,
                        paymentInfo.paymentID, paymentInfo.custID, paymentInfo.creditCardNumber, paymentInfo.cardExpiry, paymentInfo.cardCVV,
                        paymentInfo.cardHolderFirstName, paymentInfo.cardHolderLastName, paymentInfo.cardHolderPhoneNumber,
                        paymentInfo.cardHolderAddressLine1, paymentInfo.cardHolderAddressLine2, paymentInfo.cardHolderAddressLine3,
                        paymentInfo.cardHolderCity, paymentInfo.cardHolderState, paymentInfo.cardHolderZip, paymentInfo.cardHolderCountry,
                        paymentInfo.cardHolderEmail, paymentInfo.cardHolderDateOfBirth
                    };
                    tokenizedPaymentWriter.writeNext(tokenizedPaymentData, true);
                }

            }

            // Load data into vault
            if (customer_loader!=null) {
                String customer_skyflow_id = customer_loader.loadObjectIntoVault(customer);

                // Write Customer Data to CSV
                String[] tokenizedCustomerData = {
                    customer_skyflow_id,
                    customer.custID, customer.firstName, customer.lastName, customer.email, customer.phoneNumber, customer.dateOfBirth, 
                    customer.addressLine1, customer.addressLine2, customer.addressLine3, customer.city, customer.state, customer.zip, customer.country
                };
                tokenizedCustomerWriter.writeNext(tokenizedCustomerData, true);
            }

            // Progress indicator
            if ((i + 1) % 10000 == 0) {
                System.out.println("Generated " + (i + 1) + " customers.");
            }
        }
    }
}

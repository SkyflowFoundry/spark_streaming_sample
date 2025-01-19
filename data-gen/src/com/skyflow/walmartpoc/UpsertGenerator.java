
package com.skyflow.walmartpoc;

import java.io.FileReader;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.github.javafaker.Faker;
import com.opencsv.CSVReader;

public class UpsertGenerator {

    private final ArrayList<Customer> seeded_customers;
    private final ArrayList<PaymentInfo> seeded_payments;
    private final Faker faker = new Faker();
    private final List<CountryZipCityState> czcs = CountryZipCityState.loadData("US.tsv");
    private final Random random = new Random();
    private final Map<String, List<PaymentInfo>> custIDToPaymentsMap;

    // the probabilities for the upsert operation
    private final float one_card_fraction;
    private final float two_card_fraction;
    private final float insert_customer_probability;
    private final float add_payment_probability;

    public UpsertGenerator(Config config, CSVReader customerReader, CSVReader paymentReader) throws Exception {
        one_card_fraction = config.fake_data.one_card_fraction;
        two_card_fraction = config.fake_data.two_card_fraction;
        insert_customer_probability = config.upsert_probabilities.insert_customer_probability;
        add_payment_probability = config.upsert_probabilities.add_payment_probability;

        customerReader.readNext(); // Read the header
        // Ideally, we should check that the header is correct!

        seeded_customers = new ArrayList<>();
        custIDToPaymentsMap = new HashMap<>();

        String[] customerData;
        while ((customerData = customerReader.readNext()) != null) {
            Customer customer = new Customer(customerData);
            seeded_customers.add(customer);
            custIDToPaymentsMap.put(customer.custID, new LinkedList<>());
        }

        paymentReader.readNext(); // Read the header
        // Ideally, we should check that the header is correct!

        seeded_payments = new ArrayList<>();

        String[] paymentData;
        while ((paymentData = paymentReader.readNext()) != null) {
            PaymentInfo paymentInfo = new PaymentInfo(paymentData);
            seeded_payments.add(paymentInfo);
            custIDToPaymentsMap.get(paymentInfo.custID).add(paymentInfo);
        }
    }

    public Map.Entry<Customer, List<PaymentInfo>> genUpserts() {
        Customer customer;
        float r = random.nextFloat();
        if (r < insert_customer_probability) {
            // Generate a new Customer using Faker
            customer = new Customer(faker,czcs);
            custIDToPaymentsMap.put(customer.custID, new LinkedList<>());


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
                seeded_payments.add(paymentInfo);
                custIDToPaymentsMap.get(paymentInfo.custID).add(paymentInfo);
            }

            return new AbstractMap.SimpleImmutableEntry<>(customer, custIDToPaymentsMap.get(customer.custID));

        } else {
            // Choose an existing customer at random to modify
            int randomIndex = random.nextInt(seeded_customers.size());
            customer = seeded_customers.get(randomIndex);

            if (r < insert_customer_probability + add_payment_probability) {
                // Add a new payment card to the customer's account
                // Generate Payment Card Data
                PaymentInfo paymentInfo = new PaymentInfo(customer, faker);
                seeded_payments.add(paymentInfo);
                custIDToPaymentsMap.get(paymentInfo.custID).add(paymentInfo);

                List<PaymentInfo> paymentList = new LinkedList<>();
                paymentList.add(paymentInfo);
                return new AbstractMap.SimpleImmutableEntry<>(null, paymentList);                
            } else {
                // Modify the customer. Here, we only modify the email, we can modify more fields
                String newEmail = faker.internet().emailAddress();
                customer.email = newEmail;
                for (PaymentInfo p : custIDToPaymentsMap.get(customer.custID)) {
                    p.cardHolderEmail = newEmail;
                }

                return new AbstractMap.SimpleImmutableEntry<>(customer, custIDToPaymentsMap.get(customer.custID));

            }
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Please provide three arguments: the path to a config file, the output directory, and the number of upserts.");
            return;
        }

        String configFilePath = args[0];
        String outputDir = args[1];
        int n;
        try {
            n = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format for the number of upserts. Please provide an integer.");
            return;
        }

        Config config = Config.load(configFilePath);

        try (CSVReader customerReader = new CSVReader(new FileReader(Paths.get(outputDir, config.seed_data.customers_file).toString()));
             CSVReader paymentReader = new CSVReader(new FileReader(Paths.get(outputDir,config.seed_data.payments_file).toString()))) {

            UpsertGenerator upsertGenerator = new UpsertGenerator(config, customerReader, paymentReader);
            for (int i = 0; i < n; i++) {
                Map.Entry<Customer, List<PaymentInfo>> upsert = upsertGenerator.genUpserts();
                Customer upsertedCustomer = upsert.getKey();
                List<PaymentInfo> upsertedPayments = upsert.getValue();

                String customerString = (upsertedCustomer != null) ? upsertedCustomer.toString() : "null";
                StringBuilder paymentsString = new StringBuilder();

                if (upsertedPayments != null) {
                    for (PaymentInfo payment : upsertedPayments) {
                        paymentsString.append(payment.toString()).append(", ");
                    }
                    // Remove trailing comma and space if there are any payments
                    if (paymentsString.length() > 0) {
                        paymentsString.setLength(paymentsString.length() - 2);
                    }
                } else {
                    paymentsString.append("null");
                }

                System.out.println("\n\nUpsert " + (i + 1) + ": Customer: " + customerString + ", Payments: [" + paymentsString + "]");
            }
        }     
    }
}

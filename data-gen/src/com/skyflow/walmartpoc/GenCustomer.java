package com.skyflow.walmartpoc;

import com.github.javafaker.Faker;

public class GenCustomer {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please provide the number of customers to generate as an argument.");
            return;
        }

        int n;
        try {
            n = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format for the number of customers. Please provide an integer.");
            return;
        }

        // Initialize Faker
        Faker faker = new Faker();

        for (int i = 0; i < n; i++) {
            // Generate Customer Data
            Customer customer = new Customer(faker);

            // Print Customer Data in JSON format
            System.out.println(customer.toJSONString());
        }
    }

}

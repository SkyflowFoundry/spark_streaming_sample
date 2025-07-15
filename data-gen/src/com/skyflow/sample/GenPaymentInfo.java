package com.skyflow.sample;

import java.io.IOException;
import java.util.List;

import com.github.javafaker.Faker;

public class GenPaymentInfo {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Please provide the number of payment info objects to generate as an argument.");
            return;
        }

        int n;
        try {
            n = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid number format for the number of payment infos. Please provide an integer.");
            return;
        }

        // Initialize Faker & supporting stuff
        List<CountryZipCityState> czcs = CountryZipCityState.loadData("US.tsv");
        Faker faker = new Faker();
        Customer customer = new Customer(faker, czcs);
        for (int i = 0; i < n; i++) {
            // Generate Data
            PaymentInfo data = new PaymentInfo(customer, faker);

            // Print Data in JSON format
            System.out.println(data.toJSONString());
        }
    }

}

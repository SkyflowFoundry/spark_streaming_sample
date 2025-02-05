package com.skyflow.walmartpoc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.yaml.snakeyaml.Yaml;

public class Config {
    public static class SeedData {
        public String output_directory;
        public String customers_file;
        public String payments_file;
        public String catalog_file;
        public String transactions_file;
        public String consent_file;
    }
    public static class FakeData {
        // How many cards does a customer have? Probabilities that must add to <=1. Diff from 1 is 3 cards prob
        public float one_card_fraction;
        public float two_card_fraction;

        // How many txns does a customer have? Probabilities that must add to <=1. Diff from 1 is 10 txns prob
        public float zero_txn_fraction;
        public float two_txn_fraction;
        // public float ten_txn_fraction; Do not specify. Calculated from above
    }
    public static class UpsertProbabilities {
        public float insert_customer_probability;
        public float add_payment_probability;
    }
    public FakeData fake_data;
    public SeedData seed_data;
    public UpsertProbabilities upsert_probabilities;


    public static Config load(String yamlFilePath) throws IOException {
        Yaml yaml = new Yaml();

        try (InputStream in = new FileInputStream(yamlFilePath)) {
            Config config = yaml.loadAs(in, Config.class);
            return config;
        }
    }

    public Config() {}
}

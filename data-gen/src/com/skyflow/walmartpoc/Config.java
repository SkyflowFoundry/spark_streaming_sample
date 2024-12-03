package com.skyflow.walmartpoc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.yaml.snakeyaml.Yaml;

public class Config {
    public static class SeedData {
        public int total_customer_count;
        public String output_directory;
        public String customers_file;
        public String payments_file;
        public boolean load_to_vault;
        public String tokenized_customers_file;
        public String tokenized_payments_file;
    }
    public static class FakeData {
        public float one_card_fraction;
        public float two_card_fraction;
    }
    public static class VaultConfig {
        public String mgmt_url;
        public String account_id;
        public String workspace_id;
        public String vault_id;
        public String vault_url;
        public String private_key_file;
        public int max_rows_in_batch;

    }
    public FakeData fake_data;
    public SeedData seed_data;
    public VaultConfig vault;


    public static Config load(String yamlFilePath) throws IOException {
        Yaml yaml = new Yaml();

        try (InputStream in = new FileInputStream(yamlFilePath)) {
            Config config = yaml.loadAs(in, Config.class);
            return config;
        }
    }

    public Config() {}
}

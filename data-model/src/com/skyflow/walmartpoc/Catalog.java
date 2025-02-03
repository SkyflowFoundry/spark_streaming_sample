package com.skyflow.walmartpoc;

import com.github.javafaker.Faker;

public class Catalog implements SerializableDeserializable {
    String sku_id;
    String category;
    String MHMD_flag;

    Catalog(Faker faker) {
        this.sku_id = faker.internet().uuid();
        this.category = "category-" + faker.internet().uuid();
        this.MHMD_flag = faker.bool().bool() ? "yes" : "no";
    }

    Catalog(String jsonString) {
        try {
            org.json.simple.parser.JSONParser parser = new org.json.simple.parser.JSONParser();
            org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);

            this.sku_id = (String) jsonObject.get("sku_id");
            this.category = (String) jsonObject.get("category");
            this.MHMD_flag = (String) jsonObject.get("MHMD_flag");
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public String toJSONString() {
        return "{" +
                "\"sku_id\":\"" + sku_id + "\"," +
                "\"category\":\"" + category + "\"," +
                "\"MHMD_flag\":\"" + MHMD_flag + "\"" +
                "}";
    }

    private static final String CSV_HEADER[] = new String[]{"sku_id","category","MHMD_flag"};
    @Override
    public String[] toCsvRecord() {
        String[] catalogData = {
            sku_id, 
            category, 
            MHMD_flag
        };
        return catalogData;
    }
    
    static Catalog fromCsvRecord(String[] csvRecord) {
        throw new UnsupportedOperationException("not implemented");
    }

    static String[] getCsvHeader() {
        return CSV_HEADER;
    }
}

package com.skyflow.sample;

import com.github.javafaker.Faker;

@HudiConfig(recordkey_field="sku_id",precombinekey_field="lastupdate_ts",partitionpathkey_field="MHMD_flag")
public class Catalog implements SerializableDeserializable {
    String sku_id;
    String category;
    String MHMD_flag;
    Long lastupdate_ts;

    Catalog(Faker faker) {
        this.sku_id = faker.internet().uuid();
        this.category = "category-" + faker.internet().uuid();
        this.MHMD_flag = faker.bool().bool() ? "yes" : "no";

        this.lastupdate_ts = System.currentTimeMillis();
    }

    Catalog(String[] csvRecord) {
        if (csvRecord.length != 3) {
            throw new IllegalArgumentException("CSV record must have exactly 3 fields.");
        }
        this.sku_id = csvRecord[0];
        this.category = csvRecord[1];
        this.MHMD_flag = csvRecord[2];

        // We don't read lastupdate_ts ourselves. If we do write this object
        // somewhere, like a Kafka stream, we are supposed to update lastupdate_ts
    }
    public Catalog(String jsonString) {
        try {
            org.json.simple.parser.JSONParser parser = new org.json.simple.parser.JSONParser();
            org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);

            this.sku_id = (String) jsonObject.get("sku_id");
            this.category = (String) jsonObject.get("category");
            this.MHMD_flag = (String) jsonObject.get("MHMD_flag");
            this.lastupdate_ts = (Long) jsonObject.get("lastupdate_ts");
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public String toString() {
        return "Catalog{" +
                "sku_id='" + sku_id + '\'' +
                ", category='" + category + '\'' +
                ", MHMD_flag='" + MHMD_flag + '\'' +
                ", lastupdate_ts=" + lastupdate_ts +
                '}';
    }

    @Override
    public String toJSONString() {
        return "{" +
                "\"sku_id\":\"" + sku_id + "\"," +
                "\"category\":\"" + category + "\"," +
                "\"MHMD_flag\":\"" + MHMD_flag + "\"," +
                "\"lastupdate_ts\":" + lastupdate_ts + "" +
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
    
    public static Catalog fromCsvRecord(String[] csvRecord) {
        return new Catalog(csvRecord);
    }

    static String[] getCsvHeader() {
        return CSV_HEADER;
    }
}

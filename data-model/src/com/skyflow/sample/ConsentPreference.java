package com.skyflow.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.github.javafaker.Faker;

@HudiConfig(recordkey_field="startIDValue",precombinekey_field="lastupdate_ts",partitionpathkey_field="domain")
public class ConsentPreference implements SerializableDeserializable {
    String startIDType;
    String startIDValue;
    String idType;
    String idValue;
    boolean consentOptOut;
    String purpose;
    String domain;
    Long lastupdate_ts;

    ConsentPreference(String startIDType, String startIDValue, String idType, String idValue, Faker faker) {
        this.startIDType = startIDType;
        this.startIDValue = startIDValue;
        this.idType = idType;
        this.idValue = idValue;
        this.consentOptOut = faker.bool().bool();

        Random rng = new Random();
        List<String> purposes = new ArrayList<>(Arrays.asList("Marketing and Analytics", "Personalization", "Ads", "Measurement", "Optimization/ Model training", "Fraud", "ACC_Marketing"));
        Collections.shuffle(purposes, rng);
        int randomSize = faker.number().numberBetween(1, purposes.size() + 1);
        this.purpose = String.join(",",purposes.subList(0, randomSize));
        
        this.domain = faker.options().option("SampleCorp US", "ACC");

        this.lastupdate_ts = System.currentTimeMillis();
    }

    ConsentPreference(String[] csvRecord) {
        if (csvRecord.length != 7) {
            throw new IllegalArgumentException("CSV record must have exactly 7 fields.");
        }
        this.startIDType = csvRecord[0];
        this.startIDValue = csvRecord[1];
        this.idType = csvRecord[2];
        this.idValue = csvRecord[3];
        this.consentOptOut = Boolean.parseBoolean(csvRecord[4]);
        this.purpose = csvRecord[5];
        this.domain = csvRecord[6];

        // We don't read lastupdate_ts ourselves. If we do write this object
        // somewhere, like a Kafka stream, we are suppoed to update lastupdate_ts
    }

    public ConsentPreference(String jsonString) {
        try {
            org.json.simple.parser.JSONParser parser = new org.json.simple.parser.JSONParser();
            org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);

            this.startIDType = (String) jsonObject.get("startIDType");
            this.startIDValue = (String) jsonObject.get("startIDValue");
            this.idType = (String) jsonObject.get("idType");
            this.idValue = (String) jsonObject.get("idValue");
            this.consentOptOut = (Boolean) jsonObject.get("consentOptOut");
            this.purpose = (String) jsonObject.get("purpose");
            this.domain = (String) jsonObject.get("domain");
            this.lastupdate_ts = (Long) jsonObject.get("lastupdate_ts");
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public String toString() {
        return "ConsentPreference{" +
                "startIDType='" + startIDType + '\'' +
                ", startIDValue='" + startIDValue + '\'' +
                ", idType='" + idType + '\'' +
                ", idValue='" + idValue + '\'' +
                ", consentOptOut=" + consentOptOut +
                ", purpose='" + purpose + '\'' +
                ", domain='" + domain + '\'' +
                ", lastupdate_ts=" + lastupdate_ts +
                '}';
    }

    @Override
    public String toJSONString() {
        return "{" +
                "\"startIDType\":\"" + startIDType + "\"," +
                "\"startIDValue\":\"" + startIDValue + "\"," +
                "\"idType\":\"" + idType + "\"," +
                "\"idValue\":\"" + idValue + "\"," +
                "\"consentOptOut\":" + consentOptOut + "," +
                "\"purpose\":\"" + purpose + "\"," +
                "\"domain\":\"" + domain + "\"," +
                "\"lastupdate_ts\":" + lastupdate_ts + "" +
                "}";
    }

    private static final String CSV_HEADER[] = new String[]{
        "start_id_type", "start_id_value", "id_type", "id_value",
        "consent_opt_out", "purpose", "domain"};
    @Override
    public String[] toCsvRecord() {
        String[] consentData = {
            startIDType, startIDValue, idType, idValue,
            String.valueOf(consentOptOut), purpose, domain
        };
        return consentData;
    }
    
    public static ConsentPreference fromCsvRecord(String[] csvRecord) {
        return new ConsentPreference(csvRecord);
    }

    static String[] getCsvHeader() {
        return CSV_HEADER;
    }
}

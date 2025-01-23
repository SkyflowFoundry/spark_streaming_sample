package com.skyflow.walmartpoc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.github.javafaker.Faker;

public class ConsentPreference implements SerializableDeserializable {
    String startIDType;
    String startIDValue;
    String idType;
    String idValue;
    boolean consentOptOut;
    String purpose;
    String domain;

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
        
        this.domain = faker.options().option("Walmart US", "ACC");
    }

    @Override
    public String toJSONString() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJSONString'");
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
    
    static ConsentPreference fromCsvRecord(String[] csvRecord) {
        throw new UnsupportedOperationException("not implemented");
    }

    static String[] getCsvHeader() {
        return CSV_HEADER;
    }
}

package com.skyflow.walmartpoc;

import com.github.javafaker.Faker;

public class Catalog implements SerializableDeserializable {
    String sku_id;
    String category;
    boolean MHMD_flag;

    Catalog(Faker faker) {
        this.sku_id = faker.internet().uuid();
        this.category = faker.options().option("catA", "catB", "catC");
        this.MHMD_flag = faker.bool().bool();
    }

    @Override
    public String toJSONString() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJSONString'");
    }

    private static final String CSV_HEADER[] = new String[]{"sku_id","category","MHMD_flag"};
    @Override
    public String[] toCsvRecord() {
        String[] catalogData = {
            sku_id, 
            category, 
            MHMD_flag ? "yes" : "no"
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

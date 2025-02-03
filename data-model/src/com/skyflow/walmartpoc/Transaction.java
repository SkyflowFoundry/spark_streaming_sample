package com.skyflow.walmartpoc;

import java.util.Date;

import com.github.javafaker.Faker;

@HudiConfig(recordkey_field="orderID",precombinekey_field="orderDate")
public class Transaction implements SerializableDeserializable {
    String customerID;
    String orderID;
    String orderItemID;
    String sku_id;
    Date orderDate;
    String MHMD_flag;

    Transaction(Customer customer, Catalog item, Faker faker) {
        this.customerID = customer.custID;
        this.sku_id = item.sku_id;
        this.orderID = faker.internet().uuid();
        this.orderItemID = faker.internet().uuid();
        this.orderDate = faker.date().past(365, java.util.concurrent.TimeUnit.DAYS);
        this.MHMD_flag = faker.bool().bool() ? "yes" : "no";
    }

    public Transaction(String jsonString) {
        try {
            org.json.simple.parser.JSONParser parser = new org.json.simple.parser.JSONParser();
            org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);

            this.customerID = (String) jsonObject.get("customerID");
            this.orderID = (String) jsonObject.get("orderID");
            this.orderItemID = (String) jsonObject.get("orderItemID");
            this.sku_id = (String) jsonObject.get("sku_id");
            this.orderDate = new Date((Long) jsonObject.get("orderDate"));
            this.MHMD_flag = (String) jsonObject.get("MHMD_flag");
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public String toJSONString() {
        return "{" +
                "\"customerID\":\"" + customerID + "\"," +
                "\"orderID\":\"" + orderID + "\"," +
                "\"orderItemID\":\"" + orderItemID + "\"," +
                "\"sku_id\":\"" + sku_id + "\"," +
                "\"orderDate\":" + orderDate.getTime() + "," +
                "\"MHMD_flag\":\"" + MHMD_flag + "\"" +
                "}";
    }

    private static final String CSV_HEADER[] = new String[]{
        "cust_id","order_id","order_item_id","sku_id","order_date","MHMD_flag"};
    @Override
    public String[] toCsvRecord() {
        String[] transactionData = {
            customerID,
            orderID,
            orderItemID,
            sku_id,
            orderDate.toInstant().toString(),
            MHMD_flag
        };
        return transactionData;
    }
    
    static Transaction fromCsvRecord(String[] csvRecord) {
        throw new UnsupportedOperationException("not implemented");
    }

    static String[] getCsvHeader() {
        return CSV_HEADER;
    }
}
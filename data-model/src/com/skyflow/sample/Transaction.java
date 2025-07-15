package com.skyflow.sample;

import com.github.javafaker.Faker;
import com.skyflow.iface.VaultColumn;
import com.skyflow.iface.VaultObject;

@HudiConfig(recordkey_field="orderID",precombinekey_field="lastupdate_ts",partitionpathkey_field="sku_id")
@VaultObject("transaction")
public class Transaction implements SerializableDeserializable {
    @VaultColumn String customerID;
    String orderID;
    String orderItemID;
    String sku_id;
    String orderDate;
    String MHMD_flag;
    Long lastupdate_ts;

    Transaction(Customer customer, Catalog item, Faker faker, double mhmd_yes_fraction) {
        this.customerID = customer.custID;
        this.sku_id = item.sku_id;
        this.orderID = faker.internet().uuid();
        this.orderItemID = faker.internet().uuid();
        this.orderDate = faker.date().past(365, java.util.concurrent.TimeUnit.DAYS).toString();
        this.MHMD_flag = faker.random().nextDouble() < mhmd_yes_fraction ? "yes" : "no";

        this.lastupdate_ts = System.currentTimeMillis();
    }

    Transaction(String[] csvRecord) {
        if (csvRecord.length != 6) {
            throw new IllegalArgumentException("CSV record must have exactly 6 fields.");
        }
        this.customerID = csvRecord[0];
        this.orderID = csvRecord[1];
        this.orderItemID = csvRecord[2];
        this.sku_id = csvRecord[3];
        this.orderDate = csvRecord[4];
        this.MHMD_flag = csvRecord[5];

        // We don't read lastupdate_ts ourselves. If we do write this object
        // somewhere, like a Kafka stream, we are suppoed to update lastupdate_ts
    }

    public Transaction(String jsonString) {
        try {
            org.json.simple.parser.JSONParser parser = new org.json.simple.parser.JSONParser();
            org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);

            this.customerID = (String) jsonObject.get("customerID");
            this.orderID = (String) jsonObject.get("orderID");
            this.orderItemID = (String) jsonObject.get("orderItemID");
            this.sku_id = (String) jsonObject.get("sku_id");
            this.orderDate = (String) jsonObject.get("orderDate");
            this.MHMD_flag = (String) jsonObject.get("MHMD_flag");
            this.lastupdate_ts = (Long) jsonObject.get("lastupdate_ts");
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "customerID='" + customerID + '\'' +
                ", orderID='" + orderID + '\'' +
                ", orderItemID='" + orderItemID + '\'' +
                ", sku_id='" + sku_id + '\'' +
                ", orderDate=" + orderDate +
                ", MHMD_flag='" + MHMD_flag + '\'' +
                ", lastupdate_ts=" + lastupdate_ts +
                '}';
    }

    @Override
    public String toJSONString() {
        return "{" +
                "\"customerID\":\"" + customerID + "\"," +
                "\"orderID\":\"" + orderID + "\"," +
                "\"orderItemID\":\"" + orderItemID + "\"," +
                "\"sku_id\":\"" + sku_id + "\"," +
                "\"orderDate\":\"" + orderDate + "\"," +
                "\"MHMD_flag\":\"" + MHMD_flag + "\"," +
                "\"lastupdate_ts\":" + lastupdate_ts + "" +
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
            orderDate,
            MHMD_flag
        };
        return transactionData;
    }
    
    public static Transaction fromCsvRecord(String[] csvRecord) {
        return new Transaction(csvRecord);
    }

    static String[] getCsvHeader() {
        return CSV_HEADER;
    }
}
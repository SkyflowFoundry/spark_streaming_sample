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
    boolean MHMD_flag;

    Transaction(Customer customer, Catalog item, Faker faker) {
        this.customerID = customer.custID;
        this.sku_id = item.sku_id;
        this.orderID = faker.internet().uuid();
        this.orderItemID = faker.internet().uuid();
        this.orderDate = faker.date().past(365, java.util.concurrent.TimeUnit.DAYS);
        this.MHMD_flag = faker.bool().bool();
    }
    
    @Override
    public String toJSONString() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJSONString'");
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
            MHMD_flag ? "yes" : "no"
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
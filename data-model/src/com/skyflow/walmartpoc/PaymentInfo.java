
package com.skyflow.walmartpoc;

import java.util.UUID;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.github.javafaker.Faker;
import com.skyflow.iface.VaultColumn;
import com.skyflow.iface.VaultObject;

@VaultObject("customerpaymentdetails")
@HudiConfig(recordkey_field="paymentID",precombinekey_field="lastupdate_ts")
public class PaymentInfo implements SerializableDeserializable {
    @VaultColumn(upsertColumn=true) String paymentID;
    String custID;
    @VaultColumn String creditCardNumber;
    String cardExpiry; // Should be vaulted!!!
    @VaultColumn("card_cvv") String cardCVV;
    @VaultColumn String cardHolderFirstName;
    @VaultColumn String cardHolderLastName;
    @VaultColumn String cardHolderPhoneNumber;
    @VaultColumn String cardHolderAddressLine1;
    @VaultColumn String cardHolderAddressLine2;
    @VaultColumn String cardHolderAddressLine3;
    @VaultColumn String cardHolderCity;
    @VaultColumn String cardHolderState;
    @VaultColumn String cardHolderZip;
    String cardHolderCountry;
    @VaultColumn String cardHolderEmail;
    @VaultColumn String cardHolderDateOfBirth;
    Long lastupdate_ts;


    PaymentInfo(Customer customer, Faker faker) {
        this.paymentID = UUID.randomUUID().toString();
        this.custID = customer.custID;
        this.creditCardNumber = faker.finance().creditCard().replaceAll("-", "");
        this.cardExpiry = String.format("%02d/%d", faker.number().numberBetween(1, 12), faker.number().numberBetween(2024, 2030));
        this.cardCVV = String.format("%03d", faker.number().numberBetween(100, 999));

        // Use customer's name and address for cardholder details
        this.cardHolderFirstName = customer.firstName;
        this.cardHolderLastName = customer.lastName;
        this.cardHolderPhoneNumber = customer.phoneNumber;
        this.cardHolderAddressLine1 = customer.addressLine1;
        this.cardHolderAddressLine2 = customer.addressLine2;
        this.cardHolderAddressLine3 = customer.addressLine3;
        this.cardHolderCity = customer.city;
        this.cardHolderState = customer.state;
        this.cardHolderZip = customer.zip;
        this.cardHolderCountry = customer.country;
        this.cardHolderEmail = customer.email;
        this.cardHolderDateOfBirth = customer.dateOfBirth;

        this.lastupdate_ts = System.currentTimeMillis();
    }

    PaymentInfo(String[] csvRecord) {
        if (csvRecord.length != 17) {
            throw new IllegalArgumentException("CSV record must have exactly 17 fields.");
        }
        this.paymentID = csvRecord[0];
        this.custID = csvRecord[1];
        this.creditCardNumber = csvRecord[2];
        this.cardExpiry = csvRecord[3];
        this.cardCVV = csvRecord[4];
        this.cardHolderFirstName = csvRecord[5];
        this.cardHolderLastName = csvRecord[6];
        this.cardHolderPhoneNumber = csvRecord[7];
        this.cardHolderAddressLine1 = csvRecord[8];
        this.cardHolderAddressLine2 = csvRecord[9];
        this.cardHolderAddressLine3 = csvRecord[10];
        this.cardHolderCity = csvRecord[11];
        this.cardHolderState = csvRecord[12];
        this.cardHolderZip = csvRecord[13];
        this.cardHolderCountry = csvRecord[14];
        this.cardHolderEmail = csvRecord[15];
        this.cardHolderDateOfBirth = csvRecord[16];

        // We don't read lastupdate_ts ourselves. If we do write this object
        // somewhere, like a Kafka stream, we are suppoed to update lastupdate_ts
    }

    public PaymentInfo(String jsonString) {
        try {
            JSONParser parser = new org.json.simple.parser.JSONParser();
            JSONObject jsonObject = (org.json.simple.JSONObject) parser.parse(jsonString);

            this.paymentID = (String) jsonObject.get("paymentID");
            this.custID = (String) jsonObject.get("custID");
            this.creditCardNumber = (String) jsonObject.get("creditCardNumber");
            this.cardExpiry = (String) jsonObject.get("cardExpiry");
            this.cardCVV = (String) jsonObject.get("cardCVV");
            this.cardHolderFirstName = (String) jsonObject.get("cardHolderFirstName");
            this.cardHolderLastName = (String) jsonObject.get("cardHolderLastName");
            this.cardHolderPhoneNumber = (String) jsonObject.get("cardHolderPhoneNumber");
            this.cardHolderAddressLine1 = (String) jsonObject.get("cardHolderAddressLine1");
            this.cardHolderAddressLine2 = (String) jsonObject.get("cardHolderAddressLine2");
            this.cardHolderAddressLine3 = (String) jsonObject.get("cardHolderAddressLine3");
            this.cardHolderCity = (String) jsonObject.get("cardHolderCity");
            this.cardHolderState = (String) jsonObject.get("cardHolderState");
            this.cardHolderZip = (String) jsonObject.get("cardHolderZip");
            this.cardHolderCountry = (String) jsonObject.get("cardHolderCountry");
            this.cardHolderEmail = (String) jsonObject.get("cardHolderEmail");
            this.cardHolderDateOfBirth = (String) jsonObject.get("cardHolderDateOfBirth");
            this.lastupdate_ts = (Long) jsonObject.get("lastupdate_ts");
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public String toString() {
        return "PaymentInfo{" +
                "paymentID='" + paymentID + '\'' +
                ", custID='" + custID + '\'' +
                ", creditCardNumber='" + creditCardNumber + '\'' +
                ", cardExpiry='" + cardExpiry + '\'' +
                ", cardCVV='" + cardCVV + '\'' +
                ", cardHolderFirstName='" + cardHolderFirstName + '\'' +
                ", cardHolderLastName='" + cardHolderLastName + '\'' +
                ", cardHolderPhoneNumber='" + cardHolderPhoneNumber + '\'' +
                ", cardHolderAddressLine1='" + cardHolderAddressLine1 + '\'' +
                ", cardHolderAddressLine2='" + cardHolderAddressLine2 + '\'' +
                ", cardHolderAddressLine3='" + cardHolderAddressLine3 + '\'' +
                ", cardHolderCity='" + cardHolderCity + '\'' +
                ", cardHolderState='" + cardHolderState + '\'' +
                ", cardHolderZip='" + cardHolderZip + '\'' +
                ", cardHolderCountry='" + cardHolderCountry + '\'' +
                ", cardHolderEmail='" + cardHolderEmail + '\'' +
                ", cardHolderDateOfBirth='" + cardHolderDateOfBirth + '\'' +
                ", lastupdate_ts=" + lastupdate_ts +
                '}';
    }

    @Override
    public  String toJSONString() {
        return "{" +
                "\"paymentID\":\"" + paymentID + "\"," +
                "\"custID\":\"" + custID + "\"," +
                "\"creditCardNumber\":\"" + creditCardNumber + "\"," +
                "\"cardExpiry\":\"" + cardExpiry + "\"," +
                "\"cardCVV\":\"" + cardCVV + "\"," +
                "\"cardHolderFirstName\":\"" + cardHolderFirstName + "\"," +
                "\"cardHolderLastName\":\"" + cardHolderLastName + "\"," +
                "\"cardHolderPhoneNumber\":\"" + cardHolderPhoneNumber + "\"," +
                "\"cardHolderAddressLine1\":\"" + cardHolderAddressLine1 + "\"," +
                "\"cardHolderAddressLine2\":\"" + cardHolderAddressLine2 + "\"," +
                "\"cardHolderAddressLine3\":\"" + cardHolderAddressLine3 + "\"," +
                "\"cardHolderCity\":\"" + cardHolderCity + "\"," +
                "\"cardHolderState\":\"" + cardHolderState + "\"," +
                "\"cardHolderZip\":\"" + cardHolderZip + "\"," +
                "\"cardHolderCountry\":\"" + cardHolderCountry + "\"," +
                "\"cardHolderEmail\":\"" + cardHolderEmail + "\"," +
                "\"cardHolderDateOfBirth\":\"" + cardHolderDateOfBirth + "\"" +
                "\"lastupdate_ts\":" + lastupdate_ts + "" +
                "}";
        }

        private static final String CSV_HEADER[] = new String[]{
            "payment_id", "cust_id", "credit_card_number", "card_expiry", "card_cvv",
            "cardholder_first_name", "cardholder_last_name", "cardholder_phone_number",
            "cardholder_address_line_1", "cardholder_address_line_2", "cardholder_address_line_3",
            "cardholder_city", "cardholder_state", "cardholder_zip", "cardholder_country",
            "cardholder_email", "cardholder_date_of_birth"};
        @Override
        public String[] toCsvRecord() {
            String[] paymentData = {
                paymentID, custID, creditCardNumber, cardExpiry, cardCVV,
                cardHolderFirstName, cardHolderLastName, cardHolderPhoneNumber,
                cardHolderAddressLine1, cardHolderAddressLine2, cardHolderAddressLine3,
                cardHolderCity, cardHolderState, cardHolderZip, cardHolderCountry,
                cardHolderEmail, cardHolderDateOfBirth
            };
            return paymentData;
        }

        static PaymentInfo fromCsvRecord(String[] csvRecord) {
            return new PaymentInfo(csvRecord);
        }

        static String[] getCsvHeader() {
            return CSV_HEADER;
        }
    }

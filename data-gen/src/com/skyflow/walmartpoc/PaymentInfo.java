
package com.skyflow.walmartpoc;

import java.util.UUID;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.github.javafaker.Faker;

public class PaymentInfo implements JsonSerializable {
    static final String PAYMENT_CSV_HEADER[] = new String[]{"PaymentID", "CustID", "CreditCardNumber", "CardExpiry", "CardCVV", "CardHolderFirstName", "CardHolderLastName", "CardHolderPhoneNumber", "CardHolderAddressLine1", "CardHolderAddressLine2", "CardHolderAddressLine3", "CardHolderCity", "CardHolderState", "CardHolderZip", "CardHolderCountry", "CardHolderEmail", "CardHolderDateOfBirth"};
    public static final String TABLE_NAME = "customerpaymentdetails";
    public static final String UPSERT_COLUMN = "paymentid";

    String paymentID;
    String custID;
    String creditCardNumber;
    String cardExpiry;
    String cardCVV;
    String cardHolderFirstName;
    String cardHolderLastName;
    String cardHolderPhoneNumber;
    String cardHolderAddressLine1;
    String cardHolderAddressLine2;
    String cardHolderAddressLine3;
    String cardHolderCity;
    String cardHolderState;
    String cardHolderZip;
    String cardHolderCountry;
    String cardHolderEmail;
    String cardHolderDateOfBirth;

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
                "}";
        }

    public void replaceFieldsFromJson(String json) {
        try {
            JSONParser parser = new JSONParser();
            JSONObject jsonObject = (JSONObject) parser.parse(json);
            replaceFieldsFromVault(jsonObject);
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public void replaceFieldsFromVault(JSONObject jsonObject) {
        if (jsonObject.containsKey("paymentid")) {
            this.paymentID = (String) jsonObject.get("paymentid");
        }
        if (jsonObject.containsKey("customerid")) {
            this.custID = (String) jsonObject.get("customerid");
        }
        if (jsonObject.containsKey("creditcardnumber")) {
            this.creditCardNumber = (String) jsonObject.get("creditcardnumber");
        }
        if (jsonObject.containsKey("cardExpiry")) {
            this.cardExpiry = (String) jsonObject.get("cardExpiry");
        }
        if (jsonObject.containsKey("card_cvv")) {
            this.cardCVV = (String) jsonObject.get("card_cvv");
        }
        if (jsonObject.containsKey("cardholderfirstname")) {
            this.cardHolderFirstName = (String) jsonObject.get("cardholderfirstname");
        }
        if (jsonObject.containsKey("cardholderlastname")) {
            this.cardHolderLastName = (String) jsonObject.get("cardholderlastname");
        }
        if (jsonObject.containsKey("cardholderphonenumber")) {
            this.cardHolderPhoneNumber = (String) jsonObject.get("cardholderphonenumber");
        }
        if (jsonObject.containsKey("cardholderaddressline1")) {
            this.cardHolderAddressLine1 = (String) jsonObject.get("cardholderaddressline1");
        }
        if (jsonObject.containsKey("cardholderaddressline2")) {
            this.cardHolderAddressLine2 = (String) jsonObject.get("cardholderaddressline2");
        }
        if (jsonObject.containsKey("cardholderaddressline3")) {
            this.cardHolderAddressLine3 = (String) jsonObject.get("cardholderaddressline3");
        }
        if (jsonObject.containsKey("cardHolderCity")) {
            this.cardHolderCity = (String) jsonObject.get("cardHolderCity");
        }
        if (jsonObject.containsKey("cardHolderState")) {
            this.cardHolderState = (String) jsonObject.get("cardHolderState");
        }
        if (jsonObject.containsKey("cardHolderZip")) {
            this.cardHolderZip = (String) jsonObject.get("cardHolderZip");
        }
        if (jsonObject.containsKey("cardHolderCountry")) {
            this.cardHolderCountry = (String) jsonObject.get("cardHolderCountry");
        }
        if (jsonObject.containsKey("cardholderemail")) {
            this.cardHolderEmail = (String) jsonObject.get("cardholderemail");
        }
        if (jsonObject.containsKey("cardholderdateofbirth")) {
            this.cardHolderDateOfBirth = (String) jsonObject.get("cardholderdateofbirth");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public JSONObject jsonObjectForVault() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("paymentid", this.paymentID);
        jsonObject.put("creditcardnumber", this.creditCardNumber);
        jsonObject.put("cardexpiry", this.cardExpiry);
        jsonObject.put("cardcvv", this.cardCVV);
        jsonObject.put("cardholderfirstname", this.cardHolderFirstName);
        jsonObject.put("cardholderlastname", this.cardHolderLastName);
        jsonObject.put("cardholderphonenumber", this.cardHolderPhoneNumber);
        jsonObject.put("cardholderaddressline1", this.cardHolderAddressLine1);
        jsonObject.put("cardholderaddressline2", this.cardHolderAddressLine2);
        jsonObject.put("cardholderaddressline3", this.cardHolderAddressLine3);
        jsonObject.put("cardholderemail", this.cardHolderEmail);
        jsonObject.put("cardholderdateofbirth", this.cardHolderDateOfBirth);
        return jsonObject;
    }
}

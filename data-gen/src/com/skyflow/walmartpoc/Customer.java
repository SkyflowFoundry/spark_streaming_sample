package com.skyflow.walmartpoc;

import java.util.UUID;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;
import com.skflow.iface.VaultObject;
import com.skflow.iface.VaultColumn;

@VaultObject("customeraccount")
@HudiConfig(recordkey_field="custID",precombinekey_field="lastupdate_ts")
public class Customer implements JsonSerializable {
    static final String CUSTOMER_CSV_HEADER[] = new String[]{"CustID", "FirstName", "LastName", "Email", "PhoneNumber", "DateOfBirth", "AddressLine1", "AddressLine2", "AddressLine3", "City", "State", "Zip", "Country"};
    
    @VaultColumn(value="customerid",upsertColumn=true, tokenized=false) String custID;
    @VaultColumn String firstName;
    @VaultColumn String lastName;
    @VaultColumn String email;
    @VaultColumn String phoneNumber;
    @VaultColumn String dateOfBirth;
    @VaultColumn String addressLine1;
    @VaultColumn String addressLine2;
    @VaultColumn String addressLine3;
    String city;
    String state;
    String zip;
    String country;
    Long lastupdate_ts;

    Customer(Faker faker) {
        this.custID = UUID.randomUUID().toString();
        Name name = faker.name();
        this.firstName = name.firstName();
        this.lastName = name.lastName();
        this.email = faker.internet().emailAddress();
        this.phoneNumber = faker.phoneNumber().phoneNumber();
        this.dateOfBirth = new java.text.SimpleDateFormat("yyyy-MM-dd").format(faker.date().birthday(18, 80)); // Random age between 18 and 80

        Address address = faker.address();
        this.addressLine1 = address.streetAddress();
        this.addressLine2 = ""; // Optional
        this.addressLine3 = ""; // Optional
        this.city = address.city();
        this.state = address.state();
        this.zip = address.zipCode();
        this.country = address.country();

        this.lastupdate_ts = System.currentTimeMillis();
    }

    Customer(String[] csvRecord) {
        if (csvRecord.length != 13) {
            throw new IllegalArgumentException("CSV record must have exactly 13 fields.");
        }
        this.custID = csvRecord[0];
        this.firstName = csvRecord[1];
        this.lastName = csvRecord[2];
        this.email = csvRecord[3];
        this.phoneNumber = csvRecord[4];
        this.dateOfBirth = csvRecord[5];
        this.addressLine1 = csvRecord[6];
        this.addressLine2 = csvRecord[7];
        this.addressLine3 = csvRecord[8];
        this.city = csvRecord[9];
        this.state = csvRecord[10];
        this.zip = csvRecord[11];
        this.country = csvRecord[12];
    }

    public Customer(String jsonString) {
        try {
            org.json.simple.parser.JSONParser parser = new JSONParser();
            org.json.simple.JSONObject jsonObject = (JSONObject) parser.parse(jsonString);

            this.custID = (String) jsonObject.get("custID");
            this.firstName = (String) jsonObject.get("firstName");
            this.lastName = (String) jsonObject.get("lastName");
            this.email = (String) jsonObject.get("email");
            this.phoneNumber = (String) jsonObject.get("phoneNumber");
            this.dateOfBirth = (String) jsonObject.get("dateOfBirth");
            this.addressLine1 = (String) jsonObject.get("addressLine1");
            this.addressLine2 = (String) jsonObject.get("addressLine2");
            this.addressLine3 = (String) jsonObject.get("addressLine3");
            this.city = (String) jsonObject.get("city");
            this.state = (String) jsonObject.get("state");
            this.zip = (String) jsonObject.get("zip");
            this.country = (String) jsonObject.get("country");
            this.lastupdate_ts = (Long) jsonObject.get("lastupdate_ts");
        } catch (org.json.simple.parser.ParseException e) {
            throw new IllegalArgumentException("Invalid JSON string", e);
        }
    }

    @Override
    public String toString() {
        return "Customer{" +
                "custID='" + custID + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", email='" + email + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", dateOfBirth='" + dateOfBirth + '\'' +
                ", addressLine1='" + addressLine1 + '\'' +
                ", addressLine2='" + addressLine2 + '\'' +
                ", addressLine3='" + addressLine3 + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", zip='" + zip + '\'' +
                ", country='" + country + '\'' +
                ", lastupdate_ts=" + lastupdate_ts +
                '}';
    }

    @Override
    public String toJSONString() {
        return "{" +
                "\"custID\":\"" + custID + "\"," +
                "\"firstName\":\"" + firstName + "\"," +
                "\"lastName\":\"" + lastName + "\"," +
                "\"email\":\"" + email + "\"," +
                "\"phoneNumber\":\"" + phoneNumber + "\"," +
                "\"dateOfBirth\":\"" + dateOfBirth + "\"," +
                "\"addressLine1\":\"" + addressLine1 + "\"," +
                "\"addressLine2\":\"" + addressLine2 + "\"," +
                "\"addressLine3\":\"" + addressLine3 + "\"," +
                "\"city\":\"" + city + "\"," +
                "\"state\":\"" + state + "\"," +
                "\"zip\":\"" + zip + "\"," +
                "\"country\":\"" + country + "\"" +
                "\"lastupdate_ts\":" + lastupdate_ts + "" +
                "}";
    }
}

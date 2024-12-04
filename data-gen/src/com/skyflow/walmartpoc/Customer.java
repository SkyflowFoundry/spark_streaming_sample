package com.skyflow.walmartpoc;

import java.util.UUID;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;

public class Customer {
    static final String CUSTOMER_CSV_HEADER[] = new String[]{"CustID", "FirstName", "LastName", "Email", "PhoneNumber", "DateOfBirth", "AddressLine1", "AddressLine2", "AddressLine3", "City", "State", "Zip", "Country"};

    String custID;
    String firstName;
    String lastName;
    String email;
    String phoneNumber;
    String dateOfBirth;
    String addressLine1;
    String addressLine2;
    String addressLine3;
    String city;
    String state;
    String zip;
    String country;

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
                '}';
    }
}

package com.skyflow.walmartpoc;

public class SerializationDeserializationTest {
    public static void main(String[] args) {
        // Create instances of Faker for generating random data
        com.github.javafaker.Faker faker = new com.github.javafaker.Faker();

        // Create a Customer instance
        Customer customer = new Customer(new String[]{
            faker.idNumber().valid(),
            faker.name().firstName(),
            faker.name().lastName(),
            faker.internet().emailAddress(),
            faker.phoneNumber().phoneNumber(),
            faker.date().birthday().toString(),
            faker.address().streetAddress(),
            faker.address().secondaryAddress(),
            faker.address().buildingNumber(),
            faker.address().city(),
            faker.address().state(),
            faker.address().zipCode(),
            faker.address().country()
        });

        // Create a Catalog instance
        Catalog catalog = new Catalog(new String[]{
            faker.internet().uuid(),
            "category-" + faker.internet().uuid(),
            faker.bool().bool() ? "yes" : "no"
        });

        // Create a Transaction instance
        Transaction transaction = new Transaction(customer, catalog, faker, 0.5);

        // Serialize and deserialize Customer
        String customerJson = customer.toJSONString();
        Customer deserializedCustomer = new Customer(customerJson);

        // Serialize and deserialize Catalog
        String catalogJson = catalog.toJSONString();
        Catalog deserializedCatalog = new Catalog(catalogJson);

        // Serialize and deserialize Transaction
        String transactionJson = transaction.toJSONString();
        Transaction deserializedTransaction = new Transaction(transactionJson);

        // Print the original and deserialized objects
        System.out.println("Original Customer: " + customer);
        System.out.println("Deserialized Customer: " + deserializedCustomer);

        System.out.println("Original Catalog: " + catalog);
        System.out.println("Deserialized Catalog: " + deserializedCatalog);

        System.out.println("Original Transaction: " + transaction);
        System.out.println("Deserialized Transaction: " + deserializedTransaction);
    }

}

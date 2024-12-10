package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.github.javafaker.Faker;


public class KafkaPublisher<T extends JsonSerializable> implements AutoCloseable {

    final KafkaProducer<String, String> producer;
    final String topic;

    KafkaPublisher(String brokerServer, String topic, Class<T> clazz) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServer);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // AWS MSK specific config
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        this.topic = topic;

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    public void send(T object, Callback callback) throws IOException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, object.toJSONString());
        producer.send(record, callback);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalArgumentException("Please provide the broker server and topic name as command line arguments.");
        }
        
        String brokerServer = args[0];
        String topicName = args[1];

        try (KafkaPublisher<Customer> producer = new KafkaPublisher<>(brokerServer, topicName, Customer.class);) {
            Customer customer = new Customer(new Faker());

            System.out.println("sending message...");
            producer.send(customer, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Message sending failed: " + exception.getMessage());
                    exception.printStackTrace();
                } else {
                    System.out.println("Message sent successfully to topic " + metadata.topic() + " partition " + metadata.partition() + " with offset " + metadata.offset());
                }
            });
        }
        System.out.println("sent message...");
    }
}

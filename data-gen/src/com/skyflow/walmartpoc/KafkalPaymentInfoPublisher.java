package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javafaker.Faker;


public class KafkalPaymentInfoPublisher<T extends SerializableDeserializable> implements AutoCloseable {

    final KafkaProducer<String, String> producer;
    final String topic;

    KafkalPaymentInfoPublisher(String brokerServer, String topic, Class<T> clazz) {
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

        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss");
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.log.org.slf4j.MDC", "instanceId");
        Logger logger = LoggerFactory.getLogger(KafkalPaymentInfoPublisher.class);

        // Initialize Faker & supporting stuff
        List<CountryZipCityState> czcs = CountryZipCityState.loadData("US.tsv");
        Faker faker = new Faker();

        try (KafkalPaymentInfoPublisher<PaymentInfo> producer = new KafkalPaymentInfoPublisher<>(brokerServer, topicName, PaymentInfo.class);) {
            PaymentInfo paymentInfo = new PaymentInfo(new Customer(faker, czcs), faker);

            logger.info("sending message...");
            producer.send(paymentInfo, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Message sending failed: {}", exception.getMessage(), exception);
                } else {
                    logger.info("Message sent successfully to topic {} partition {} with offset {}", metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        }
        logger.info("sent message...");
    }
}

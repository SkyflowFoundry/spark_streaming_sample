package com.skyflow.walmartpoc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

public class KafkaPublisher<T extends SerializableDeserializable> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final int[] partitions;
    private int partionIndex=0;

    private final CollectorAndReporter stats;
    private final ValueDatum numRecords;
    private final ValueDatum numErrors;
    
    KafkaPublisher(Class<T> clazz, boolean runLocally,
                    String kafkaBootstrap, String topicBase, String partitions,
                    CollectorAndReporter stats) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        if (runLocally) {
            kafkaProperties.put("security.protocol", "PLAINTEXT");
        } else {
            // AWS MSK specific config
            kafkaProperties.put("security.protocol", "SASL_SSL");
            kafkaProperties.put("sasl.mechanism", "AWS_MSK_IAM");
            kafkaProperties.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            kafkaProperties.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }
        kafkaProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProperties.put(ProducerConfig.RETRIES_CONFIG, 0);
        kafkaProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100000);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.topic = topicBase + "-" + clazz.getSimpleName();
        
        int[] kafkaPartitions = null;
        if (!partitions.isEmpty()) {
            String[] partitionStrings = partitions.split(",");
            List<Integer> partitionList = new ArrayList<>();
            for (String partitionString : partitionStrings) {
                partitionString = partitionString.trim();
                if (partitionString.contains("-")) {
                    String[] range = partitionString.split("-");
                    int start = Integer.parseInt(range[0].trim());
                    int end = Integer.parseInt(range[1].trim());
                    for (int j = start; j <= end; j++) {
                        partitionList.add(j);
                    }
                } else {
                    partitionList.add(Integer.parseInt(partitionString));
                }
            }
            kafkaPartitions = partitionList.stream().mapToInt(Integer::intValue).toArray();
        }
        this.partitions = kafkaPartitions;

        Map<String, String> dimensionMap = new HashMap<>();
        dimensionMap.put("object", clazz.getSimpleName());
        this.numRecords = stats.createOrGetUniqueMetric("numRecordsPublished", dimensionMap, StandardUnit.COUNT, ValueDatum.class);
        this.numErrors = stats.createOrGetUniqueMetric("numPublishErrors", dimensionMap, StandardUnit.COUNT, ValueDatum.class);
        this.stats = stats;
        
        producer = new KafkaProducer<>(kafkaProperties);
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    public void publish(T obj) throws IOException {
        Integer partition = null;
        if (partitions!=null) {
            partition = partitions[partionIndex];
            partionIndex = (partionIndex + 1) % partitions.length;
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, null, obj.toJSONString());
        logger.debug("Sending to partition {}", partition);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                numErrors.increment();
                logger.error("Message sending failed: {}", exception.getMessage(), exception);
            } else {
                numRecords.increment();
                logger.debug("Message sent successfully to topic {} partition {} with offset {}", metadata.topic(), metadata.partition(), metadata.offset());
            }
            stats.pollAndReport(false);
        });
    }
}
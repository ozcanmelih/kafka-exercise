package com.mozcan.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaPropertiesFactory {

    public static Properties createKafkaProducerProperties(boolean safeProducer, boolean highThroughputProducer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer properties
        if (safeProducer) {
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
            properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
            // Kafka 2.0 > 1.1 so we can keep this as 5. Use 1 otherwise
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        }

        if (highThroughputProducer) {
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 2014));
        }

        return properties;
    }

    public static Properties createKafkaConsumerProperties(String bootstrapServer, String groupId, boolean autoCommit, Integer maxPollRecords) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        if (groupId != null) {
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (!autoCommit) {
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
        if (maxPollRecords != null) {
            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString());
        }

        return properties;
    }
}

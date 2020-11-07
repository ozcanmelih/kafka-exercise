package com.mozcan.kafka.basics.consumer;

import com.mozcan.kafka.common.KafkaConstants;
import com.mozcan.kafka.common.KafkaPropertiesFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

    public static void main(String[] args) {

        Properties properties = KafkaPropertiesFactory.createKafkaConsumerProperties(KafkaConstants.BOOTSTRAP_SERVER, null, true, null);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition partitionToReadFrom = new TopicPartition(KafkaConstants.FIRST_TOPIC, 0);
        long offsetForReadFrom = 15L;
        consumer.assign(Collections.singletonList(partitionToReadFrom));

        consumer.seek(partitionToReadFrom, offsetForReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFar++;
                logger.info("Key: " + record.key() + ", Value:" + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if (numberOfMessagesReadSoFar > numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application");
    }
}

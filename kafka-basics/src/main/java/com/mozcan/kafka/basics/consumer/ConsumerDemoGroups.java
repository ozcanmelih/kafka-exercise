package com.mozcan.kafka.basics.consumer;

import com.mozcan.kafka.common.KafkaConstants;
import com.mozcan.kafka.common.KafkaPropertiesFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    public static void main(String[] args) {

        Properties properties = KafkaPropertiesFactory.createKafkaConsumerProperties(KafkaConstants.BOOTSTRAP_SERVER, KafkaConstants.GROUP_ID_5, true, null);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(KafkaConstants.FIRST_TOPIC));

        while (true) {
            //consumer.poll(100); // new in Kafka 2.0.0
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value:" + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}

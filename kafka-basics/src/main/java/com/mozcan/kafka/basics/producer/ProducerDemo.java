package com.mozcan.kafka.basics.producer;

import com.mozcan.kafka.common.KafkaConstants;
import com.mozcan.kafka.common.KafkaPropertiesFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        Properties properties = KafkaPropertiesFactory.createKafkaProducerProperties(false, false);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.FIRST_TOPIC, "hello world");
        producer.send(record);

        producer.flush();
        producer.close();
    }
}

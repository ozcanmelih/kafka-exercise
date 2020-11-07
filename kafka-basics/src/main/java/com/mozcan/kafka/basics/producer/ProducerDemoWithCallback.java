package com.mozcan.kafka.basics.producer;

import com.mozcan.kafka.common.KafkaConstants;
import com.mozcan.kafka.common.KafkaPropertiesFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        Properties properties = KafkaPropertiesFactory.createKafkaProducerProperties(false, false);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(KafkaConstants.FIRST_TOPIC, "hello world " + Integer.toString(i));
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.toString());
                    } else {
                        logger.error("Error while processing" + exception);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}

package com.mozcan.kafka.basics.consumer;

import com.mozcan.kafka.common.KafkaConstants;
import com.mozcan.kafka.common.KafkaPropertiesFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();

    }

    private ConsumerDemoWithThread() {
    }

    private void run() {

        // latch for dealing multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerRunnable = new ConsumerRunnable(KafkaConstants.BOOTSTRAP_SERVER, KafkaConstants.GROUP_ID_6, KafkaConstants.FIRST_TOPIC, latch);

        // start thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerRunnable implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;
            Properties properties = KafkaPropertiesFactory.createKafkaConsumerProperties(bootstrapServer, groupId, true, null);
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value:" + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException exception) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consume.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}

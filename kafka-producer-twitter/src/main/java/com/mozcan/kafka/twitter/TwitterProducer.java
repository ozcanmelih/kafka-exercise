package com.mozcan.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.mozcan.kafka.common.KafkaConstants;
import com.mozcan.kafka.common.KafkaPropertiesFactory;
import com.mozcan.kafka.common.Messages;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private static final String CONSUMER_KEY = "twitter.consumerKey";
    private static final String CONSUMER_SECRET = "twitter.consumerSecret";
    private static final String ACCESS_TOKEN = "twitter.accessToken";
    private static final String ACCESS_TOKEN_SECRET = "twitter.accessTokenSecret";

    List<String> terms = Lists.newArrayList("kafka", "java", "redis", "microservices", "bitcoin", "usa");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("stopping twitter client...");
            twitterClient.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!...");
        }));

        // on a different thread, or multiple different threads....
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("An error occurred.", e);
                twitterClient.stop();
            }

            if (msg != null) {
                producer.send(new ProducerRecord<>(KafkaConstants.TWITTER_TOPIC, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            logger.error("Something bad happened.", exception);
                        }
                    }
                });
            }
        }

        logger.info("End of application.");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties kafkaConsumerProperties = KafkaPropertiesFactory.createKafkaProducerProperties(true, true);
        return new KafkaProducer<>(kafkaConsumerProperties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        endpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication auth = new OAuth1(Messages.getAsString(CONSUMER_KEY),
                Messages.getAsString(CONSUMER_SECRET),
                Messages.getAsString(ACCESS_TOKEN),
                Messages.getAsString(ACCESS_TOKEN_SECRET));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}

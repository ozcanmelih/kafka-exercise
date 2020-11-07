package com.mozcan.kafka.elasticsearch;

import com.google.gson.JsonParser;
import com.mozcan.kafka.common.KafkaConstants;
import com.mozcan.kafka.common.KafkaPropertiesFactory;
import com.mozcan.kafka.common.Messages;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private static final String ELASTIC_HOST = "elastic.host";
    private static final String ELASTIC_ACCESS_KEY = "elastic.accessKey";
    private static final String ELASTIC_ACCESS_SECRET = "elastic.accessSecret";
    private static final String TWITTER_INDEX = "twitter";

    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        new ElasticSearchConsumer().run();
    }

    public void run() {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer(KafkaConstants.TWITTER_TOPIC);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            int count = records.count();
            logger.info("Received " + count + " records.");

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {

                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                String value = (String) record.value();
                String tweetId = extractIdFromTweets(value);

                IndexRequest indexRequest = new IndexRequest(TWITTER_INDEX)
                        .source(value, XContentType.JSON)
                        .id(tweetId);

                bulkRequest.add(indexRequest);
            }

            if (count > 0) {
                try {
                    client.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (IOException | ElasticsearchException e) {
                    logger.error("Error occurred", e);
                }

                logger.info("Commiting offsets...");
                consumer.commitAsync();
                logger.info("Offsets have been commited.");
                takeRest(1000);
            }
        }

        // client.close();
    }

    private static void takeRest(int milis) {
        try {
            Thread.sleep(milis);
        } catch (InterruptedException e) {
            logger.error("An error occurred while taking rest.", e);
        }
    }

    private String extractIdFromTweets(String tweetJson) {
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    private KafkaConsumer<String, String> createConsumer(String topic) {
        Properties properties = KafkaPropertiesFactory.createKafkaConsumerProperties(KafkaConstants.BOOTSTRAP_SERVER, KafkaConstants.GROUP_ID_ELASTICSEARCH, true, 10);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }

    private RestHighLevelClient createClient() {
        String apiKeyId = Messages.getAsString(ELASTIC_ACCESS_KEY);
        String apiKeySecret = Messages.getAsString(ELASTIC_ACCESS_SECRET);
        String apiKeyAuth = Base64.getEncoder()
                .encodeToString((apiKeyId + ":" + apiKeySecret).getBytes(StandardCharsets.UTF_8));

        Header[] defaultHeaders =
                new Header[]{new BasicHeader("Authorization", "ApiKey " + apiKeyAuth)};

        RestClientBuilder builder = RestClient.builder(new HttpHost(Messages.getAsString(ELASTIC_HOST), 443, "https"))
                .setDefaultHeaders(defaultHeaders);

        return new RestHighLevelClient(builder);
    }
}

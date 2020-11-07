package com.mozcan.kafka.streams;

import com.google.gson.JsonParser;
import com.mozcan.kafka.common.KafkaConstants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {

        Properties kafkaStreamsProperties = createKafkaStreamsProperties();

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> stream = streamsBuilder.stream(KafkaConstants.TWITTER_TOPIC);
        KStream<String, String> filteredStream = stream.filter((k, jsonTweet) -> extractUserFollowerFromTweet(jsonTweet) > 10000);
        filteredStream.to(KafkaConstants.TWITTER_IMPORTANT_TOPIC);

        // build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), kafkaStreamsProperties);

        kafkaStreams.start();
    }

    private static Integer extractUserFollowerFromTweet(String tweetJson) {
        try {
            return jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception exception) {
            return 0;
        }
    }

    public static Properties createKafkaStreamsProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID_STREAMS);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return properties;
    }
}

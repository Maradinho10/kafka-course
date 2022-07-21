package com.maradinho.kafka.wikimedia.streams;

import com.maradinho.kafka.wikimedia.streams.processor.BotCountStreamBuilder;
import com.maradinho.kafka.wikimedia.streams.processor.EventCountTimeseriesBuilder;
import com.maradinho.kafka.wikimedia.streams.processor.WebsiteCountStreamBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WikimediaStreamsApp {
    public static final String BOOSTRAP_SERVER = "127.0.0.1:9092";

    public static final String APPLICATION_ID = "wikimedia-stats-application";

    private static final Logger log = LoggerFactory.getLogger(WikimediaStreamsApp.class);

    private static final Properties properties;

    private static final String INPUT_TOPIC = "wikimedia.recentchange";

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> changeJsonStream = builder.stream(INPUT_TOPIC);

        BotCountStreamBuilder botCountStreamBuilder = new BotCountStreamBuilder(changeJsonStream);
        botCountStreamBuilder.setup();

        WebsiteCountStreamBuilder websiteCountStreamBuilder = new WebsiteCountStreamBuilder(changeJsonStream);
        websiteCountStreamBuilder.setup();

        EventCountTimeseriesBuilder eventCountTimeseriesBuilder = new EventCountTimeseriesBuilder(changeJsonStream);
        eventCountTimeseriesBuilder.setup();

        final Topology topology = builder.build();
        log.info("Topology: {}", topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }

    static {
        properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOSTRAP_SERVER);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }
}

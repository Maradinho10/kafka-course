package com.maradinho.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static final String TOPIC = "wikimedia.recentchange";
    public static final String SOURCE_URL = "https://stream.wikimedia.org./v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {
        Properties properties = KafkaHelper.buildKafkaDefaultProperties(StringSerializer.class.getName(), StringSerializer.class.getName());

        // Creates the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Setting high throughput producer properties
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        EventHandler eventHandler = new WikimediaChangeHandler(producer, TOPIC); // TODO
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(SOURCE_URL));
        EventSource eventSource = builder.build();

        // Start producer in another thread
        eventSource.start();

        // Produce during 10 mins
        TimeUnit.MINUTES.sleep(10);
    }
}

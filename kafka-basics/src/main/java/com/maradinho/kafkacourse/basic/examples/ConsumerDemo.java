package com.maradinho.kafkacourse.basic.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.EARLIEST_OFFSET_RESET_CONFIG;
import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.STRING_SERIALIZER;
import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.TOPIC_NAME;

public class ConsumerDemo {
    private static  final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("Kafka consumer execution starts");
        String consumerGroup = "consumer-group-1";

        // Creates consumer
        Properties properties = KafkaBasicsHelper.buildKafkaProperties(STRING_SERIALIZER, STRING_SERIALIZER,
                consumerGroup, EARLIEST_OFFSET_RESET_CONFIG);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribes consumer to topic
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        // Polls for new data
        while (true) {
            log.info("Polling...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: {}, value: {}, partition: {}, offset: {}",
                        record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}

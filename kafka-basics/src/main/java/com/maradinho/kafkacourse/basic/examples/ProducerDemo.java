package com.maradinho.kafkacourse.basic.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.*;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("Kafka producer!");
        Properties properties = KafkaBasicsHelper.buildKafkaProperties(STRING_SERIALIZER, STRING_SERIALIZER);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, "Hello world.");

        // Send data - async operation
        producer.send(producerRecord);

        // Flush and close producer - async
        producer.flush();
        producer.close();
    }
}

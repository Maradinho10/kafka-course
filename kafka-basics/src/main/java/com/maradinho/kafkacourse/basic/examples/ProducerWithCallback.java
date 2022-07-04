package com.maradinho.kafkacourse.basic.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.STRING_SERIALIZER;
import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.TOPIC_NAME;

public class ProducerWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class);

    public static void main(String[] args) {
        log.info("Kafka producer with callback!");
        Properties properties = KafkaBasicsHelper.buildKafkaProperties(STRING_SERIALIZER, STRING_SERIALIZER);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Send data - async operation
        IntStream.rangeClosed(0, 9).forEach(i -> {
            // Create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, null, "Hello world " + i);
            producer.send(producerRecord, new KafkaSimpleCallback());

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });


        // Flush and close producer - async
        producer.flush();

        producer.close();
    }
}

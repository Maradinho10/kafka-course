package com.maradinho.kafkacourse.basic.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSimpleCallback implements Callback {
    private static final Logger log = LoggerFactory.getLogger(KafkaSimpleCallback.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            log.info("Message received\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.offset());
        } else {
            log.error("Error while producing", exception);
        }
    }
}

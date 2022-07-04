package com.maradinho.kafkacourse.basic.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.COOPERATIVE_STICKY_ASSIGNOR;
import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.EARLIEST_OFFSET_RESET_CONFIG;
import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.STRING_SERIALIZER;
import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.TOPIC_NAME;

public class ConsumerDemoWithShutdown {
    private static  final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);

    public static void main(String[] args) {
        log.info("Kafka consumer with shutdown hook execution starts");
        String consumerGroup = "consumer-group-2";

        // Creates consumer
        Properties properties = KafkaBasicsHelper.buildKafkaProperties(STRING_SERIALIZER, STRING_SERIALIZER,
                consumerGroup, EARLIEST_OFFSET_RESET_CONFIG, COOPERATIVE_STICKY_ASSIGNOR);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Reference to current thread
        final Thread mainThread = Thread.currentThread();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown, calling consumer.wakeup() method...");
            consumer.wakeup();

            // Join main thread to allow the execution of the code in main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            // Subscribes consumer to topic
            consumer.subscribe(Collections.singleton(TOPIC_NAME));

            // Polls for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: {}, value: {}, partition: {}, offset: {}",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close();       // Will commit the offsets if needed
            log.info("Consumer gracefully closed.");
        }


    }
}

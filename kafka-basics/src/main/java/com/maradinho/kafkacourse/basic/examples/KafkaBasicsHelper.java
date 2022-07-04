package com.maradinho.kafkacourse.basic.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;

import static com.maradinho.kafkacourse.basic.examples.KafkaConstants.BOOTSTRAP_SERVER_ADDRESS;

public class KafkaBasicsHelper {

    public static Properties buildKafkaProperties(String keySerializer, String valueSerializer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_ADDRESS);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        return properties;
    }

    public static Properties buildKafkaProperties(String keySerializer, String valueSerializer, String group,
                                           String offsetResetConfig, String partitionAssignmentStrategy) {
        Properties properties = buildKafkaProperties(keySerializer, valueSerializer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);

        Optional <String> partitionAssignmentOpt = Optional.ofNullable(partitionAssignmentStrategy);
        partitionAssignmentOpt.ifPresent(s -> properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, s));

        return properties;
    }

    public static Properties buildKafkaProperties(String keySerializer, String valueSerializer, String group, String offsetResetConfig) {
        return buildKafkaProperties(keySerializer, valueSerializer, group, offsetResetConfig, null);
    }


}

package com.maradinho.kafkacourse.basic.examples;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaConstants {
    public static final String BOOTSTRAP_SERVER_ADDRESS = "127.0.0.1:9092";

    public static final String TOPIC_NAME = "demo_java";

    public static final String STRING_SERIALIZER = StringSerializer.class.getName();

    public static final String EARLIEST_OFFSET_RESET_CONFIG = "earliest";

    public static final String COOPERATIVE_STICKY_ASSIGNOR = CooperativeStickyAssignor.class.getName();
}

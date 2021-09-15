package com.berako.berakademy.kafka.tutorial1.consumer;

import java.util.Arrays;

public class MainApplicationConsumer {
    private static final String FIRST_TOPIC = "first_topic";
    private static final String SECOND_TOPIC = "second_topic";
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "fifth_app";
    private static final String OFFSET_RESET_STRATEGY = "earliest";

    public static void main(String[] args) {
        KafkaConsumerImpl<String, String> kafkaConsumer = new KafkaConsumerImpl<>(BOOTSTRAP_SERVER, GROUP_ID, OFFSET_RESET_STRATEGY);
        kafkaConsumer.subscribeToTopic(Arrays.asList(FIRST_TOPIC, SECOND_TOPIC));
    }
}

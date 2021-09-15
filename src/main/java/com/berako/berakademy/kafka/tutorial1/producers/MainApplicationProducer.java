package com.berako.berakademy.kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.Function;

public class MainApplicationProducer {
    private static final String FIRST_TOPIC = "first_topic";
    private static final String SECOND_TOPIC = "second_topic";
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public static void main(String[] args) {
        KafkaProducerImpl<String, String> kafkaProducerImpl = new KafkaProducerImpl(BOOTSTRAP_SERVER);

        sendLoopMessages(10, integer -> kafkaProducerImpl.sendData(createProducerRecordWithKey(integer)));
        sendLoopMessages(10, integer -> kafkaProducerImpl.sendData(createProducerRecord(integer)));

        kafkaProducerImpl.closeFlushing();
    }

    private static <R> void sendLoopMessages(Integer messagesAmount, Function<Integer, R> function) {
        for (int i = 0; i < messagesAmount; i++) {
            function.apply(i);
        }
    }

    private static ProducerRecord<String, String> createProducerRecord(Integer incrementMessage) {

        return new ProducerRecord<>(SECOND_TOPIC, String.format("{\"hello world\": \"%s\"}", incrementMessage));
    }

    private static ProducerRecord<String, String> createProducerRecordWithKey(Integer key) {
        return new ProducerRecord<>(
                FIRST_TOPIC,
                String.format("id_%s", key),
                String.format("{\"message\": \"%s\"}", key)
        );
    }
}

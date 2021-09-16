package com.berako.berakademy.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaConsumerImpl<K, V> {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerImpl.class);
    private final KafkaConsumer<K, V> kafkaConsumer;

    private Properties properties;

    public KafkaConsumerImpl(String bootstrapServer, String groupId, String offsetResetStrategy) {
        createConsumerProperties(bootstrapServer, groupId, offsetResetStrategy);
        this.kafkaConsumer = new KafkaConsumer<>(properties);
    }

    private void createConsumerProperties(String bootstrapServer, String groupId, String offsetResetStrategy) {
        this.properties = new Properties();

        this.properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.properties.setProperty(GROUP_ID_CONFIG, groupId);
        this.properties.setProperty(AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
    }

    public void subscribeToTopic(List<String> topics) {
        int numberOfMessagesToRead = 10;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = Boolean.TRUE;

        kafkaConsumer.subscribe(topics);

        while (keepOnReading) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<K, V> record: records){
                numberOfMessagesReadSoFar += 1;

                LOGGER.info(String.format("Topic: %s", record.topic()));
                LOGGER.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
                LOGGER.info(String.format("Partition: %s, Offset: %s", record.partition(), record.offset()));

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }

        LOGGER.info("Exiting the application");
    }

    public void fetchFromOffset(List<TopicPartition> topicsPartition, Long offsetToFetchFrom) {
        kafkaConsumer.assign(topicsPartition);

        topicsPartition.forEach(topicPartition ->
                kafkaConsumer.seek(topicPartition, offsetToFetchFrom));
    }
}

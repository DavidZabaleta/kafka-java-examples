package com.berako.berakademy.kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

import static java.util.Objects.nonNull;

public class KafkaProducerImpl<K, V> {
    private final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerImpl.class);
    private final KafkaProducer<K, V> kafkaProducer;

    private Properties properties;

    public KafkaProducerImpl(String bootstrapServer) {
        createProducerProperties(bootstrapServer);
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    private void createProducerProperties(String bootstrapServer) {
        this.properties = new Properties();

        this.properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public Future<RecordMetadata> sendData(ProducerRecord<K, V> record) {
        return kafkaProducer.send(record, (recordMetadata, e) -> {
            if (nonNull(e))
                LOGGER.error("Error while producing the message: ", e);

            LOGGER.info("Received new metadata. \n"
                    .concat(String.format("Topic: %s \n", recordMetadata.topic()))
                    .concat(String.format("Partition: %s \n", recordMetadata.partition()))
                    .concat(String.format("Offset: %s \n", recordMetadata.offset()))
                    .concat(String.format("Timestamp: %s \n", recordMetadata.timestamp()))
            );
        });
    }

    public void closeFlushing() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

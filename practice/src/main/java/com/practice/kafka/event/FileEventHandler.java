package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class.getName());

    private final KafkaProducer<String, String> kafkaProducer;
    private String topicName;
    private boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);

        if (this.sync) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("sync partition: {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        } else  {
            this.kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                if (exception != null) {
                    logger.info("async partition: {}, offset: {}, timestamp: {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                } else {
                    logger.error("exception error from broker: {}", exception.getMessage());
                }
            });
        }
    }

}

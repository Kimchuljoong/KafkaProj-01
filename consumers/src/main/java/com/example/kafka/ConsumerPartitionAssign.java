package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerPartitionAssign {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssign.class.getName());

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_01");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
//        kafkaConsumer.subscribe(List.of(topicName));
        kafkaConsumer.assign(List.of(topicPartition));

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("main program starts to exit by calling wakeup");
                kafkaConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

//        pollAutoCommit(kafkaConsumer);
//        pollCommitSync(kafkaConsumer);
        pollNoCommit(kafkaConsumer);
//        pollcommitAsync(kafkaConsumer);
    }

    private static void pollNoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000L));
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {}, record value: {} partition: {}",
                            record.key(), record.value(), record.partition());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    private static void pollcommitAsync(KafkaConsumer<String, String> kafkaConsumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000L));
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {}, record value: {} partition: {}",
                            record.key(), record.value(), record.partition());
                }
                kafkaConsumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        logger.error("offsets {} is not completed, error: {}", offsets, exception.getMessage());
                    }
                });

            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                kafkaConsumer.commitSync();
            } catch (CommitFailedException e) {
                logger.error(e.getMessage());
            }
            kafkaConsumer.close();
        }
    }


    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000L));
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {}, record value: {} partition: {}",
                            record.key(), record.value(), record.partition());
                }

                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitAsync();
                    }

                    logger.info("commit sync has been called");
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }


    private static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000L));
                for (ConsumerRecord record : consumerRecords) {
                    logger.info("record key: {}, record value: {} partition: {}",
                            record.key(), record.value(), record.partition());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            kafkaConsumer.close();
        }
    }

}

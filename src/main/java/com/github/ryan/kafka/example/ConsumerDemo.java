package com.github.ryan.kafka.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * com.github.ryan.kafka.tutorial1.ConsumerDemo
 *
 * @author Ryan Hallberg
 */
public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
        final String GROUP_ID = "my-fifth-application";
        final String OFFSET_RESET = "earliest";
        final String TOPIC = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET);

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer =
                new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> consumerRecords =
                    kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                logger.info("Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value());
                logger.info("Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
            }
        }

    }
}

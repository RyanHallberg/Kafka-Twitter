package com.github.ryan.kafka.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * com.github.ryan.kafka.tutorial1.ProducerDemo
 *
 * @author Ryan Hallberg
 */
public class ProducerDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemo.class); // create a logger for this class

        String bootstrapServers = "127.0.0.1:9092";

        // producer properties
        // notice that they are String, String key, value pairs.
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer
        // this is the current type of producer
        //KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        Producer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);



        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("first_topic", key, "Hello World");

            logger.info("key: " + key);
            // send data - asynchronous, must flush before exiting
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (exception == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata:\n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            });
        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

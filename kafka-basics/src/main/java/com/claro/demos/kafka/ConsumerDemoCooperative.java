package com.claro.demos.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoCooperative {


    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka consumer");

        String bootstrapservers = "linuxdev1:9092";
        String groupId = "my-application";
        String topic = "demo_java";

        // consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // added configuration to assume only cooperative mode
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        // consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to current thread
        final Thread mainThread = Thread.currentThread();

        // adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let´s exit by calling consumer.wakeup()");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        try {
            // subscribe to a topic
            consumer.subscribe(Collections.singletonList(topic));

            // pool for data

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info(String.format("Key: %s , Value: %s", record.key(), record.value()));
                    log.info(String.format("Partition: %s, Offset: %s", record.partition(), record.offset()));
                }
            }
        } catch (WakeupException e) {
            // an expected exception when closing a consumer
            log.info("Wake up exception");
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            // permit the offsets if needed
            consumer.close();
            log.info("The consumer is now gracefully closed");
        }


    }
}

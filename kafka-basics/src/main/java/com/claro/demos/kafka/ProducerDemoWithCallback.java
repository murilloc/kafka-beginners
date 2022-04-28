package com.claro.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {


    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        logger.info("Hello World, I am Kafka!");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linuxdev1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // send data - asynchronous
        for (int i = 1; i <= 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", String.format("Message %d: hello world!", i));
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes once a record is sent
                    if (exception == null) {
                        logger.info(String.format("Received new Metadata: \nTopic: %s\nPartition: %s\nOffset:%s\nTimestamp:%s",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset(),
                                metadata.timestamp())
                        );
                    } else {
                        logger.error("Error while producing");
                    }
                }
            });
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // flush and close the Producer
        producer.flush();
        producer.close();


    }
}

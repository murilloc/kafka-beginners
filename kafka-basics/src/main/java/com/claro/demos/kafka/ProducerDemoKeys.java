package com.claro.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {


    public static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

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
        for (int i = 1; i <= 20; i++) {
            String topic = "demo_java";
            String value = String.format("Hello I am a message %d from kafka producer with keys", i);
            String key = String.format("id_%d", i);

            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes once a record is sent
                    if (exception == null) {
                        logger.info(String.format("Received new Metadata: \nTopic: %s\nKey: %s\nPartition: %s\nOffset:%s\nTimestamp:%s",
                                metadata.topic(),
                                producerRecord.key(),
                                metadata.partition(),
                                metadata.offset(),
                                metadata.timestamp())
                        );
                    } else {
                        logger.error("Error while producing");
                    }
                }
            });
        }

        // flush and close the Producer
        producer.flush();
        producer.close();


    }
}

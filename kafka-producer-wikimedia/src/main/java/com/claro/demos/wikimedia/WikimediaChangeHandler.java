package com.claro.demos.wikimedia;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {


    public final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        logger.info("ChangeHandler is opening...");

    }

    @Override
    public void onClosed() throws Exception {

        kafkaProducer.close();

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {

        logger.info(messageEvent.getData());

        // asynchronous message
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {
        logger.info("On Comment");

    }

    @Override
    public void onError(Throwable t) {
        logger.error("Stream reading error", t);

    }

}

package com.igoreul.consumer;

import java.util.concurrent.CountDownLatch;

import com.igoreul.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);
    private String payload = null;

    @KafkaListener(topics = "${topic}")
    public void receive(User user) {
        LOGGER.info("received payload='{}'", user.toString());
        setPayload(user.toString());
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public String getPayload() {
        return payload;
    }

    private void setPayload(String payload) {
        this.payload = payload;
    }

}

package com.inventory.management.messaging;

import com.inventory.management.entity.Event;
import com.inventory.management.repository.EventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventsReceiver {
    private static final String TOPIC_ORDER = "orderUpdateTopic";
    private static final String TOPIC_PRODUCT = "productBroadcastTopic";

    private static final String TOPIC_PAYMENT = "paymentUpdateTopic";

    private static final String TOPIC_SHIPPING = "shipmentUpdateTopic";

    @Autowired
    private EventRepository repository;

    @KafkaListener(topics = TOPIC_ORDER, groupId = "event-group", containerFactory = "tranRecordListener")
    private void listenOrderEvents(Event event) throws Exception {
        log.info("Received message :" + event + " in " + TOPIC_ORDER);
        System.out.println("Received message :" + event + " in " + TOPIC_ORDER);
        repository.save(event);
    }

    @KafkaListener(topics = TOPIC_PRODUCT, groupId = "event-group", containerFactory = "tranRecordListener")
    private void listenProductEvents(Event event) throws Exception {
        log.info("Received message :" + event + " in " + TOPIC_PRODUCT);
        System.out.println("Received message :" + event + " in " + TOPIC_PRODUCT);
        repository.save(event);
    }

    @KafkaListener(topics = TOPIC_PAYMENT, groupId = "event-group", containerFactory = "tranRecordListener")
    private void listenPaymentEvents(Event event) throws Exception {
        log.info("Received message :" + event + " in " + TOPIC_ORDER);
        System.out.println("Received message :" + event + " in " + TOPIC_ORDER);
        repository.save(event);
    }

    @KafkaListener(topics = TOPIC_SHIPPING, groupId = "event-group", containerFactory = "tranRecordListener")
    private void listenShipmentEvents(Event event) throws Exception {
        log.info("Received message :" + event + " in " + TOPIC_ORDER);
        System.out.println("Received message :" + event + " in " + TOPIC_ORDER);
        repository.save(event);
    }
}

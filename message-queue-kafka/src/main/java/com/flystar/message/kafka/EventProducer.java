package com.flystar.message.kafka;

import com.flystar.data.common.Actor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zack on 6/6/2016.
 */
public class EventProducer  implements Actor{
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final KafkaConfiguration config;
    private final KafkaProducer<String,String> producer;

    public EventProducer(KafkaConfiguration config) {
        this.config = config;
        this.producer = new KafkaProducer<>(config.getProp());
    }

    @Override
    public void start() {
        log.info("Event Producer starts");
    }

    @Override
    public void stop() {
        log.info("Event Producer stops");
        this.producer.flush();
        this.producer.close();
    }

    public void publish(String topic, String message) {
        this.producer.send(new ProducerRecord<>(topic,message));
    }
}

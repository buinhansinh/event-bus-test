package com.flystar.message.kafka;

import com.flystar.data.common.Actor;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by zack on 6/6/2016.
 */
public class EventConsumer<T> implements Actor {
    private final String ID = UUID.randomUUID().toString();
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final KafkaConfiguration config;
    private final String topic;
    private final Consumer<T> consumer;
    private final Function<String,T> converter;
    private final ExecutorService es = Executors.newFixedThreadPool(2);
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public EventConsumer(KafkaConfiguration config, String topic, Consumer<T> consumer, Function<String, T> converter) {
        this.config = config;
        this.topic = topic;
        this.consumer = consumer;
        this.converter = converter;
    }

    @Override
    public void start() {
        config.getProp().put("group.id", UUID.randomUUID().toString());
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(config.getProp());
        log.info("Subscribing to topic: "+topic);
        consumer.subscribe(Arrays.asList(topic));
        final AtomicLong counter = new AtomicLong(0);
        new Thread(() -> {
            while(!stopped.get()){
                try{
                    ConsumerRecords<String,String> records  = consumer.poll(100L);
                    for(ConsumerRecord<String,String> record : records){
                        if(log.isDebugEnabled()) log.debug(ID+" receiving total events: "+counter.incrementAndGet());
                        if(log.isDebugEnabled()) log.debug(new JsonObject(record.value()).encodePrettily());
                        this.consumer.accept(this.converter.apply(record.value()));
                    }
                }catch(Exception ex){
                    log.error("error",ex);
                }

            }
            consumer.close();
            log.info("Consumer stopped");
        }).start();

    }

    @Override
    public void stop() {
        log.info("Consumer for topic: "+topic+" stopping...");
        stopped.set(true);
    }
}

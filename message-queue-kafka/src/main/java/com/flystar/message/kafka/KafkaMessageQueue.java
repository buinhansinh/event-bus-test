package com.flystar.message.kafka;

import com.flystar.data.common.Actor;
import com.flystar.queue.api.MessageQueue;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by zack on 6/6/2016.
 */
public class KafkaMessageQueue implements MessageQueue<JsonObject>,Actor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final KafkaConfiguration producerConfig;
    private final EventProducer producer;
    private final List<Actor> actors = Collections.synchronizedList(new ArrayList<>());
    private final KafkaConfiguration consumerConfig;
    private final String prefix;

    public KafkaMessageQueue(KafkaConfiguration producerConfig, KafkaConfiguration consumerConfig, String prefix) {
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
        this.prefix = prefix;
        this.producer = new EventProducer(producerConfig);
        this.actors.add(this.producer);
    }

    @Override
    public void start() {
        log.info("Kafka Message Queue starts");
        startActor(actors);

    }

    private synchronized void startActor(List<Actor> actors) {
        for(Actor actor : actors){
            actor.start();
        }
    }

    private synchronized void stopActor(List<Actor> actors){
        for(Actor actor: actors){
            actor.stop();
        }
    }

    @Override
    public void stop() {
        log.info("Kafka message queue stops");
        startActor(this.actors);
    }

    @Override
    public void send(String topic, JsonObject message) {
        this.producer.publish(topic(topic),message.encode());
    }

    @Override
    public void subscribe(String topic, Consumer<JsonObject> consumer) {
        final EventConsumer<JsonObject> eventConsumer = new EventConsumer<>(consumerConfig,topic(topic),consumer, s -> new JsonObject(s));
        this.actors.add(eventConsumer);
        eventConsumer.start();
    }

    private String topic(String topic){
       return String.format("%s-%s",prefix,topic);
    }
}

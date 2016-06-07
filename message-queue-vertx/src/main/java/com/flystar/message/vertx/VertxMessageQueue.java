package com.flystar.message.vertx;

import com.flystar.queue.api.MessageQueue;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;

import java.util.function.Consumer;

/**
 * Created by zack on 6/5/2016.
 */
public class VertxMessageQueue implements MessageQueue<JsonObject> {

    private final Vertx vertx;

    public VertxMessageQueue(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void send(String topic, JsonObject message) {
        this.vertx.eventBus().publish(topic,message);
    }

    @Override
    public void subscribe(String topic, Consumer<JsonObject> consumer) {
        final MessageConsumer<JsonObject> t = this.vertx.eventBus().consumer(topic);
        t.handler(m -> consumer.accept(m.body()));
    }
}

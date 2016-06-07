package com.flystar.queue.api;

import java.util.function.Consumer;

/**
 * Created by zack on 6/5/2016.
 */
public interface MessageQueue<T> {

    void send(String topic,T message);
    void subscribe(String topic, Consumer<T> consumer);
}

package com.flystar.message.rabbit;

import com.flystar.data.common.Actor;
import com.flystar.queue.api.MessageQueue;
import com.google.common.base.Charsets;
import com.rabbitmq.client.*;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Created by zack on 6/7/2016.
 */
public class RabbitMessageQueue implements MessageQueue<JsonObject>,Actor {

    private final String EXCHANGE_NAME;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final RabbitMQConfiguration config;
    private final Deque<Connection> connections = new LinkedList<>();
    private final Deque<Channel> channels = new LinkedList<>();
    private final String queueName;
    private ConnectionFactory factory;

    public RabbitMessageQueue(RabbitMQConfiguration config) {
        this.config = config;
        this.EXCHANGE_NAME = config.getExchangeName();
        this.queueName = this.config.getQueueName();
    }


    @Override
    public synchronized void start() {
        try{
            log.info("Initializing RabbitMQ");
            log.info("Configuration");
            log.info(Json.encodePrettily(this.config));
            factory = getConnectionFactory();

            log.info("Creating connection pool");
            for(int i = 0; i < this.config.getConnectionNumber(); ++i){
                log.info("Creating connection: "+i);
                this.connections.add(factory.newConnection());
            }
            log.info("Creating publish channel");
            for(int i = 0; i < this.config.getPublishChannelNumber(); ++i){
                final Channel ch = getChannel();
                ch.exchangeDeclare(EXCHANGE_NAME,this.config.getExchangeType());
                this.channels.add(getChannel());
            }
        }catch(Exception ex){
            log.error("Error",ex);
            throw new RuntimeException(ex);
        }
    }

    private ConnectionFactory getConnectionFactory() {
        final ConnectionFactory fa = new ConnectionFactory();
        fa.setHost(config.getHostName());
        fa.setUsername(config.getUsername());
        fa.setPassword(config.getPassword());
        return fa;
    }

    @Override
    public synchronized void stop() {
        log.info("Stopping RabbitMQ");
        for(Connection connection : connections){
            try{
                connection.close();
            }catch(Exception ex){
                log.error("Close ",ex);
            }
        }
    }

    private Channel getChannel(){
        try{
            final Channel ch = getConnections().createChannel();
            ch.queueDeclare(this.queueName,false,false,false,null);
            ch.exchangeDeclare(EXCHANGE_NAME,this.config.getExchangeType());
            return ch;
        }catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }

    private synchronized Channel nextPublishChannel(){
        final Channel channel = channels.pollFirst();
        if(channel == null) throw new RuntimeException("No connection available");
        channels.addLast(channel);
        return channel;
    }

    private synchronized Connection getConnections() {
        final Connection connection = connections.pollFirst();
        if(connection == null) throw new RuntimeException("No connection available");
        connections.addLast(connection);
        return connection;
    }

    @Override
    public void send(String topic, JsonObject message) {
        try {
            nextPublishChannel().basicPublish(EXCHANGE_NAME,topic,null,message.encode().getBytes(Charsets.UTF_8));
        } catch (IOException e) {
            log.error("publish error ",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void subscribe(String topic, Consumer<JsonObject> consumer) {
        final Channel channel = getChannel();
        final com.rabbitmq.client.Consumer cs = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if(log.isDebugEnabled()) log.debug("Receiving message: "+envelope.getRoutingKey());
                final JsonObject message = new JsonObject(new String(body, Charsets.UTF_8));
                if(log.isDebugEnabled()) log.debug("Receiving message: "+message.encodePrettily());
                consumer.accept(message);
            }
        };
        try{
            channel.queueBind(this.queueName,this.EXCHANGE_NAME,topic);
            channel.basicConsume(queueName,true,cs);
        }catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }
}

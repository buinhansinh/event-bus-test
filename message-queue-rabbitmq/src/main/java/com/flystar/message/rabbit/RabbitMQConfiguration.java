package com.flystar.message.rabbit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Charsets;

import java.beans.Transient;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.UUID;

/**
 * Created by zack on 6/7/2016.
 */
public class RabbitMQConfiguration {

    private final int connectionNumber;
    private final String hostName;
    private final String username;
    @JsonIgnore
    private final transient String password;
    private final String exchangeName;
    private final String queueName;
    private final int publishChannelNumber;
    private final String exchangeType;

    public RabbitMQConfiguration(int connectionNumber, String hostName, String username, String password, String exchangeName, String queueName, int publishChannelNumber, String exchangeType) {
        this.connectionNumber = connectionNumber;
        this.hostName = hostName;
        this.username = username;
        this.password = password;
        this.exchangeName = exchangeName;
        this.queueName = queueName;
        this.publishChannelNumber = publishChannelNumber;
        this.exchangeType = exchangeType;
    }

    public int getConnectionNumber() {
        return connectionNumber;
    }

    public String getHostName() {
        return hostName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public int getPublishChannelNumber() {
        return publishChannelNumber;
    }

    public String getUsername() {
        return username;
    }


    @Transient
    public String getPassword() {
        return decrypt(password);
    }

    public static RabbitMQConfiguration getDefault() {
        return new RabbitMQConfiguration(1,"104.155.53.68", "flystar", "Zmx5c3Rhci10ZXN0", "FLYSTAR_EVENT", UUID.randomUUID().toString(),1, "topic");
    }

    public String getExchangeType() {
        return exchangeType;
    }

    private static String decrypt(String data){
        try {
            final String password = new String(Base64.getDecoder().decode(data.getBytes(Charsets.UTF_8)),"UTF-8");
            return password;
        } catch (UnsupportedEncodingException e) {
           throw new RuntimeException(e);
        }
    }
}

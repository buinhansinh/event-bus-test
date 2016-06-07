package com.flystar.message.kafka;

import io.vertx.core.json.JsonObject;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by zack on 6/6/2016.
 */
public class KafkaConfiguration {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private static final String KAFKA_SERVERS = "104.155.61.245:9092";
    private final Properties prop;

    public KafkaConfiguration(Properties prop) {
        this.prop = prop;
        log.info(new JsonObject((Map) prop).encodePrettily());
    }

    public static KafkaConfiguration getDefault(){
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_SERVERS);

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "10");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");
        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        return new KafkaConfiguration(props);
    }

    public Properties getProp() {
        return prop;
    }
}

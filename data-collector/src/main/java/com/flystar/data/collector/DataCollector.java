package com.flystar.data.collector;

import com.flystar.data.common.Actor;
import com.flystar.queue.api.MessageQueue;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Scanner;
import java.util.function.Consumer;

/**
 * Created by zack on 6/5/2016.
 */
public class DataCollector implements Actor {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    final static String TOPIC = "EVENT";
    private final DataSource dataSource;
    private final MessageQueue<JsonObject> messageQueue;

    public DataCollector(DataSource dataSource, MessageQueue<JsonObject> messageQueue) {
        this.dataSource = dataSource;
        this.messageQueue = messageQueue;
    }

    @Override
    public void start() {
        log.info("Data Collector starts");
        streamFromDataSource(dataSource, event -> {
            messageQueue.send(TOPIC,parseEvent(event));
        });
    }

    @Override
    public void stop() {
        log.info("Data Collector stops");
    }

    public void streamFromDataSource(DataSource dataSource, Consumer<String> consumer) {

        try(Scanner scanner = new Scanner( new BufferedReader(new InputStreamReader(dataSource.getInputStream())))){
            while(scanner.hasNextLine()){
                consumer.accept(scanner.nextLine());
            }
        }catch(Exception ex){
            log.error("Error: ",ex);
        }
        log.info("Finish reading data source");
    }
    JsonObject parseEvent(String data) {
        return new JsonObject(data);
    }
}

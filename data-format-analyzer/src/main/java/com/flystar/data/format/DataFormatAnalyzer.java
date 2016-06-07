package com.flystar.data.format;


import com.flystar.data.common.Actor;
import com.flystar.queue.api.MessageQueue;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zack on 6/5/2016.
 */
public class DataFormatAnalyzer implements Actor {
    private static final String TOPIC = "EVENT";
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Map<String,Set<EventFormat>> eventFormats = new ConcurrentHashMap<>(50);
    private final MessageQueue<JsonObject> messageQueue;
    private final File outputFile;
    private boolean stop;

    public DataFormatAnalyzer(MessageQueue<JsonObject> messageQueue, File outputFile) {
        this.messageQueue = messageQueue;
        this.outputFile = outputFile;

    }

    EventFormat getEventFormat(JsonObject event) {
        final String eventType = event.getString("type");
        return new EventFormat(eventType,event.fieldNames());
    }

    @Override
    public synchronized void start() {
        log.info("Event Format Analyzer starts");
        messageQueue.subscribe(TOPIC,this::analyzeEvent);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           stop();
        }));
    }

    @Override
    public synchronized void stop() {
        if(stop) return;
        stop = true;
        log.info("Writing output to file");
        try {
            Files.write(outputFile.toPath(), Arrays.asList(Json.encodePrettily(eventFormats)), StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("Error",e);
        }
    }

    public void analyzeEvent(JsonObject event) {
        final EventFormat ef = getEventFormat(event);
        eventFormats.computeIfAbsent(ef.getType(),eventType ->  Collections.newSetFromMap(new ConcurrentHashMap<>())).add(ef);

    }
}

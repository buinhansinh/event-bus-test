package com.flystar.data.processor.event;

import com.flystar.data.common.Chronicle;
import com.flystar.queue.api.MessageQueue;
import com.google.common.base.Strings;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.set.ChronicleSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Created by zack on 6/5/2016.
 */
public class DuplicateEventDetector extends AbstractEventProcessor<JsonObject>{
    private final static String TOPIC = "EVENT";
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Map<String,Boolean> eventIds = Chronicle.createMap(String.class,Boolean.class,UUID.randomUUID().toString(),16_000_00L);
    private final Set<String> duplicatedEvents = Chronicle.createSet(String.class,UUID.randomUUID().toString(),50000L);
    private final MessageQueue<JsonObject> messageQueue;
    private final File outputFile;


    public DuplicateEventDetector(MessageQueue<JsonObject> messageQueue, File outputFile) {
        this.messageQueue = messageQueue;
        this.outputFile = outputFile;
    }

    private void analyzeEvent(JsonObject event){
        final String eventID = event.getString("event_id");
        if(Strings.isNullOrEmpty(eventID)) return;
        if(eventIds.merge(eventID,Boolean.TRUE,(key,value) -> {
            if(value != null){
                if(log.isDebugEnabled()) log.debug("Duplicated event detected: "+eventID+ "old value = "+value);
                duplicatedEvents.add(eventID);
            }

            return true;
        }));
    }


    @Override
    protected void doStart() {
        messageQueue.subscribe(TOPIC,this::analyzeEvent);
    }

    @Override
    public void doStop() {
             log.info("DeplicateEventDetector stops");
        if(outputFile != null){
            log.info("Writing output to file");
            final List<String> events = new ArrayList<>(duplicatedEvents);
            Collections.sort(events);
            JsonObject jo = new JsonObject().put("title","There are "+duplicatedEvents.size()+" duplicated events")
                    .put("events",new JsonArray(events));
            writeJsonToFile(outputFile,jo);
        }
    }



}

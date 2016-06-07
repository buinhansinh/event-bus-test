package com.flystar.data.processor.event;

import com.flystar.data.common.Chronicle;
import com.flystar.queue.api.MessageQueue;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by zack on 6/5/2016.
 */
public class DeviceLongevityDetector extends AbstractEventProcessor<JsonObject> {
    private static final int MAX_EVENTS = 5_000_00;
    private static final String TOPIC = "EVENT";
    private static final String REPORT_DEVICE = "REPORT-DEVICE";
    private final MessageQueue<JsonObject> messageQueue;
    private final File outputFile;
    private final Map<String,AtomicLong> oldestEvent = new ConcurrentHashMap<>(MAX_EVENTS);
    private final Map<String,AtomicLong> latestEvent = new ConcurrentHashMap<>(MAX_EVENTS);

    public DeviceLongevityDetector(MessageQueue<JsonObject> messageQueue, File outputFile) {
        this.messageQueue = messageQueue;
        this.outputFile = outputFile;
    }

    @Override
    protected void doStop() {
        if(outputFile == null) return;
        analyzeDeviceWithLongestLongevity();

    }

    private void analyzeDeviceWithLongestLongevity() {
        String deviceUUID = null;
        Map<String,Long> deviceTime = new ConcurrentHashMap<>(500_000);
        oldestEvent.keySet().parallelStream().forEach(deviceID -> {
            final long lastTime = latestEvent.get(deviceID).get();
            deviceTime.put(deviceID,lastTime - oldestEvent.get(deviceID).get());
        });

        log.info("Finding max lifetime");
        final Long maxTime = deviceTime.values().parallelStream().max(Comparator.naturalOrder()).get();
        log.info("Searching for all devices having max life time");
        final List<String> devices = deviceTime.keySet().parallelStream().filter(id -> deviceTime.get(id) == maxTime).collect(Collectors.toList());
        Collections.sort(devices);
        final JsonArray maxDevices = new JsonArray();
        for(String dev : devices){
            maxDevices.add(new JsonObject().put("device_id",dev).put("oldest_event",oldestEvent.get(dev).get()).put("latest_event",latestEvent.get(dev).get()));
        }
        final JsonObject result = new JsonObject().put("maxTime",maxTime).put("devices",maxDevices).put("details",new TreeMap<>(deviceTime));
        writeJsonToFile(outputFile,result);
    }

    @Override
    protected void doStart() {
        messageQueue.subscribe(TOPIC,this::analyzeEvent);
    }

    private void analyzeEvent(JsonObject event) {
        final JsonObject time = event.getJsonObject("time");
        final String deviceID = event.getJsonObject("device").getString("device_id");
        if(log.isDebugEnabled()) log.debug(deviceID);
        final long sendTime = time.getLong("send_timestamp");
        updateOldestTime(oldestEvent.computeIfAbsent(deviceID,key -> new AtomicLong(Long.MAX_VALUE)),sendTime);
        updateLatestTime(latestEvent.computeIfAbsent(deviceID,key -> new AtomicLong(Long.MIN_VALUE)),sendTime);
    }

    private void updateLatestTime(AtomicLong time, long sendTime) {
        while(sendTime > time.get()){
            sendTime = time.getAndSet(sendTime);
        }
    }

    private void updateOldestTime(AtomicLong time, long sendTime) {
        while(sendTime < time.get()){
            sendTime = time.getAndSet(sendTime);
        }
    }

}

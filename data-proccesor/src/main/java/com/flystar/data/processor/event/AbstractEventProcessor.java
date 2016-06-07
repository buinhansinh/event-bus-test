package com.flystar.data.processor.event;

import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * Created by zack on 6/5/2016.
 */
public abstract class AbstractEventProcessor<T> implements EventProcessor<T> {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    private boolean stop = false;
    @Override
    public synchronized void start() {
        log.info(this.getClass().getName()+" starts");
        doStart();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop();
        }));
    }

    @Override
    public synchronized void stop() {
        if(stop) return;
        stop = true;
        doStop();
    }

    protected abstract void doStop();
    protected abstract void doStart();

    protected final void writeJsonToFile(File outputFile, JsonObject jo) {
        try {
            Files.write(outputFile.toPath(), Arrays.asList(jo.encodePrettily()), StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("Error",e);
        }
    }
}

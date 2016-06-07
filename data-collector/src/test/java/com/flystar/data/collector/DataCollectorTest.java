package com.flystar.data.collector;

import com.flystar.queue.api.MessageQueue;
import io.vertx.core.json.JsonObject;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by zack on 6/5/2016.
 */
public class DataCollectorTest {


    /**
     * Created by zack on 6/5/2016.
     */
    private final Logger log = Logger.getLogger(this.getClass());
    private DataCollector dataReader;
    private DummyMesageQueue messageQueue;

    @Before
    public void setUp() {
        final File file = new File(new File(".").getAbsoluteFile().getParentFile().getParentFile(),"obfuscated_data.xz");
        log.info("Testing file located at: "+file.getAbsolutePath());
        messageQueue = new DummyMesageQueue();
        dataReader = new DataCollector(new XZFileDataSource(file),messageQueue);
    }

    @Test
    public void testCanReadFileAndStreamEvent() {
        dataReader.start();
        assertThat(messageQueue.counter.get(),equalTo(1515859L));
    }

    private static class DummyMesageQueue implements MessageQueue<JsonObject>{
        final AtomicLong counter = new AtomicLong();
        @Override
        public void send(String topic, JsonObject message) {
            counter.incrementAndGet();
        }

        @Override
        public void subscribe(String topic, Consumer<JsonObject> consumer) {

        }
    }



}

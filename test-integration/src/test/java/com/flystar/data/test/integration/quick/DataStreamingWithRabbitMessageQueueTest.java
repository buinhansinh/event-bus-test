package com.flystar.data.test.integration.quick;

import com.flystar.data.collector.DataCollector;
import com.flystar.data.format.DataFormatAnalyzer;
import com.flystar.data.processor.event.DeviceLongevityDetector;
import com.flystar.data.processor.event.DuplicateEventDetector;
import com.flystar.data.test.integration.AbstractTest;
import com.flystar.data.test.integration.Clock;
import com.flystar.data.test.integration.FileDataSource;
import com.flystar.message.rabbit.RabbitMQConfiguration;
import com.flystar.message.rabbit.RabbitMessageQueue;
import com.flystar.queue.api.MessageQueue;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

/**
 * Created by zack on 6/7/2016.
 */
public class DataStreamingWithRabbitMessageQueueTest extends AbstractTest {


    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
    private File file;
    private File outputFile1;
    private File outputFile2;
    private File outputFile3;
    private File resultFile;
    private File duplictedEventFile;
    private File maxTimeFile;
    private Clock clock;

    @Before
    public void setUp() throws Exception {
        file = getFile("/quick/data.txt");
        outputFile1 = createTmpFile(PREFIX);
        outputFile2 = createTmpFile(PREFIX);
        outputFile3 = createTmpFile(PREFIX);
        resultFile = getFile("/quick/result.txt");
        duplictedEventFile = getFile("/quick/duplicateEvents.txt");
        maxTimeFile = getFile("/quick/maxDevice.txt");
        clock = new Clock();
        clock.start();
    }



    @After
    public void tearDown(){
        clock.stop();
        log.info("Test takes: "+clock.duration()+" milliseconds");
    }

    /*
    *  The RabbitQueue is working properly but the configuration ( exchange types) are not configured properly therefore clients just receive a part of the event
    *
    *This test is expected to fail
    * */
    @Test
    public void testDataProcessorAnalyzeDataStreamingFromDataCollectorViaMessageQueue() throws IOException {
        final DataCollector dataReader = new DataCollector(new FileDataSource(file), createRabbitMessageQueue());
        final DataFormatAnalyzer dataAnalyzer = new DataFormatAnalyzer(createRabbitMessageQueue(), outputFile1);
        final DuplicateEventDetector duplicateEventDetector = new DuplicateEventDetector(createRabbitMessageQueue(),outputFile2);
        final DeviceLongevityDetector deviceLongevityDetector = new DeviceLongevityDetector(createRabbitMessageQueue(),outputFile3);
        dataAnalyzer.start();
        duplicateEventDetector.start();
        deviceLongevityDetector.start();
        waitFor(FIVE_SECONDS);
        dataReader.start();
        waitFor(FIVE_SECONDS);
        dataAnalyzer.stop();
        duplicateEventDetector.stop();
        deviceLongevityDetector.stop();
        compareFileContent(resultFile,outputFile1);
        compareFileContent(duplictedEventFile,outputFile2);
        compareFileContent(maxTimeFile,outputFile3);
    }
}

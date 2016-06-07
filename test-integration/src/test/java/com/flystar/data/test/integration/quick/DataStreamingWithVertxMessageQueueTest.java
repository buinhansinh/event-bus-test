package com.flystar.data.test.integration.quick;

import com.flystar.data.collector.DataCollector;
import com.flystar.data.format.DataFormatAnalyzer;
import com.flystar.data.processor.event.DeviceLongevityDetector;
import com.flystar.data.processor.event.DuplicateEventDetector;
import com.flystar.data.test.integration.AbstractTest;
import com.flystar.data.test.integration.Clock;
import com.flystar.data.test.integration.FileDataSource;
import com.flystar.message.vertx.VertxMessageQueue;
import io.vertx.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertThat;

/**
 * Created by zack on 6/5/2016.
 */
public class DataStreamingWithVertxMessageQueueTest extends AbstractTest{
    private File file;
    private File outputFile1;
    private File outputFile2;
    private File outputFile3;
    private File resultFile;
    private File duplictedEventFile;
    private File maxTimeFile;
    private Clock clock;
    private VertxMessageQueue VertxQueue;

    @Before
    public void setUp() throws Exception {
        file = getFile("/quick/data.txt");
        outputFile1 = createTmpFile(PREFIX);
        outputFile2 = createTmpFile(PREFIX);
        outputFile3 = createTmpFile(PREFIX);
        resultFile = getFile("/quick/result.txt");
        duplictedEventFile = getFile("/quick/duplicateEvents.txt");
        maxTimeFile = getFile("/quick/maxDevice.txt");
        VertxQueue = new VertxMessageQueue(Vertx.vertx());
        clock = new Clock();
        clock.start();
    }

    @After
    public void tearDown(){
        clock.stop();
        log.info("Test takes: "+clock.duration()+" milliseconds");
    }

    @Test
    public void testDataProcessorAnalyzeDataStreamingFromDataCollectorViaMessageQueue() throws IOException, InterruptedException {
        final DataCollector dataReader = new DataCollector(new FileDataSource(file), VertxQueue);
        final DataFormatAnalyzer dataAnalyzer = new DataFormatAnalyzer(VertxQueue, outputFile1);
        final DuplicateEventDetector duplicateEventDetector = new DuplicateEventDetector(VertxQueue,outputFile2);
        final DeviceLongevityDetector deviceLongevityDetector = new DeviceLongevityDetector(VertxQueue,outputFile3);
        dataAnalyzer.start();
        duplicateEventDetector.start();
        deviceLongevityDetector.start();
        dataReader.start();
        waitFor(TEN_SECONDS);
        dataAnalyzer.stop();
        duplicateEventDetector.stop();
        deviceLongevityDetector.stop();
        compareFileContent(resultFile,outputFile1);
        compareFileContent(duplictedEventFile,outputFile2);
        compareFileContent(maxTimeFile,outputFile3);
    }




}

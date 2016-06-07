package com.flystar.data.test.integration.slow;

import com.flystar.data.collector.DataCollector;
import com.flystar.data.collector.XZFileDataSource;
import com.flystar.data.format.DataFormatAnalyzer;
import com.flystar.data.processor.event.DeviceLongevityDetector;
import com.flystar.data.processor.event.DuplicateEventDetector;
import com.flystar.data.test.integration.AbstractTest;
import com.flystar.data.test.integration.Clock;
import com.flystar.message.vertx.VertxMessageQueue;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.vertx.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Duration;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Created by zack on 6/5/2016.
 */
public class DataStreamingWithVertxMessageQueueTest extends AbstractTest {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

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
        file = new File(new File(".").getAbsoluteFile().getParentFile().getParentFile(),"obfuscated_data.xz");
        outputFile1 = createTmpFile(PREFIX2);
        outputFile2 = createTmpFile(PREFIX2);
        outputFile3 = createTmpFile(PREFIX2);
        resultFile = getFile("/slow/result.txt");
        duplictedEventFile = getFile("/slow/duplicateEvents.txt");
        maxTimeFile = getFile("/slow/maxDevice.txt");
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
        final DataCollector dataReader = new DataCollector(new XZFileDataSource(file), VertxQueue);
        final DataFormatAnalyzer dataAnalyzer = new DataFormatAnalyzer(VertxQueue, outputFile1);
        final DuplicateEventDetector duplicateEventDetector = new DuplicateEventDetector(VertxQueue,outputFile2);
        final DeviceLongevityDetector deviceLongevityDetector = new DeviceLongevityDetector(VertxQueue,outputFile3);
        dataAnalyzer.start();
        duplicateEventDetector.start();
        deviceLongevityDetector.start();
        dataReader.start();
        dataAnalyzer.stop();
        duplicateEventDetector.stop();
        deviceLongevityDetector.stop();

        compareFileContent(resultFile,outputFile1);
        compareFileContent(duplictedEventFile,outputFile2);
        compareFileContent(maxTimeFile,outputFile3);
    }

}

package com.flystar.data.test.integration.slow;

import com.flystar.data.collector.DataCollector;
import com.flystar.data.collector.XZFileDataSource;
import com.flystar.data.format.DataFormatAnalyzer;
import com.flystar.data.processor.event.DeviceLongevityDetector;
import com.flystar.data.processor.event.DuplicateEventDetector;
import com.flystar.data.test.integration.AbstractTest;
import com.flystar.data.test.integration.Clock;
import com.flystar.message.kafka.KafkaConfiguration;
import com.flystar.message.kafka.KafkaMessageQueue;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertThat;

/**
 * Created by zack on 6/5/2016.
 */
public class DataStreamingWithKafkaMessageQueueTest extends AbstractTest{

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
        file = new File(new File(".").getAbsoluteFile().getParentFile().getParentFile(),"obfuscated_data.xz");
        outputFile1 = createTmpFile(PREFIX2);
        outputFile2 = createTmpFile(PREFIX2);
        outputFile3 = createTmpFile(PREFIX2);
        resultFile = getFile("/slow/result.txt");
        duplictedEventFile = getFile("/slow/duplicateEvents.txt");
        maxTimeFile = getFile("/slow/maxDevice.txt");
        clock = new Clock();
        clock.start();
    }

    @After
    public void tearDown(){
        clock.stop();
        log.info("Test takes: "+clock.duration()+" milliseconds");
    }

    @Test
    public void testDataProcessorAnalyzeDataStreamingFromDataCollectorViaMessageQueue() throws IOException {
        final DataCollector dataReader = new DataCollector(new XZFileDataSource(file), createKafkaMessageQueue());
        final DataFormatAnalyzer dataAnalyzer = new DataFormatAnalyzer(createKafkaMessageQueue(), outputFile1);
        final DuplicateEventDetector duplicateEventDetector = new DuplicateEventDetector(createKafkaMessageQueue(),outputFile2);
        final DeviceLongevityDetector deviceLongevityDetector = new DeviceLongevityDetector(createKafkaMessageQueue(),outputFile3);
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

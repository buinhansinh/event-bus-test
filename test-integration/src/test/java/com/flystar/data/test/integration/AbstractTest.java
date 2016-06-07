package com.flystar.data.test.integration;

import com.flystar.message.kafka.KafkaConfiguration;
import com.flystar.message.kafka.KafkaMessageQueue;
import com.flystar.message.rabbit.RabbitMQConfiguration;
import com.flystar.message.rabbit.RabbitMessageQueue;
import com.flystar.queue.api.MessageQueue;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.vertx.core.json.JsonObject;
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
 * Created by zack on 6/6/2016.
 */
public class AbstractTest {

    protected final static String PREFIX = "quick_";
    protected final static String PREFIX2 = "quick_";
    protected final Duration FIVE_SECONDS = Duration.ofSeconds(5);
    protected final static Duration TEN_SECONDS = Duration.ofSeconds(10);
    protected static final Duration THIRTY_SECONDS = Duration.ofSeconds(30);
    final protected Logger log = LoggerFactory.getLogger(this.getClass());
    final protected File getFile(String fileName) throws URISyntaxException {
        return  new File(this.getClass().getResource(fileName).toURI());
    }

    final protected File createTmpFile(String prefix) throws IOException {
        final File file =  Files.createTempFile(new File(".").toPath(),prefix,".txt").toFile();
        // file.deleteOnExit();
        return file;
    }

    final protected MessageQueue<JsonObject> createRabbitMessageQueue(){
        final RabbitMessageQueue q = new RabbitMessageQueue(RabbitMQConfiguration.getDefault());
        q.start();
        return q;
    }

    final protected MessageQueue<JsonObject> createKafkaMessageQueue(){
        final KafkaMessageQueue q = new KafkaMessageQueue(KafkaConfiguration.getDefault(),KafkaConfiguration.getDefault(), "quick-test4");
        return q;
    }

    final protected void compareFileContent(File expectedFile,File computeResult) throws FileNotFoundException {
        final JsonElement result = getJson(computeResult);
        final JsonElement expectedResult = getJson(expectedFile);
        log.info("-------------------------------------------------------------------------");
        logJsonObject(result);
        logJsonObject(expectedResult);
        log.info("-------------------------------------------------------------------------");
        assertThat(expectedResult.equals(result),equalTo(true));
    }

    final protected void waitFor(Duration duration){
        try{
            log.info("Wait for remaining events to be processed");
            Thread.sleep(duration.toMillis());
        }catch(Exception ex){
            throw new RuntimeException(ex);
        }
    }

    private static JsonElement getJson(File f) throws FileNotFoundException {
        return new JsonParser().parse(new FileReader(f));
    }

    final protected void logJsonObject(Object object){
        log.info(new JsonObject(object.toString()).encodePrettily());
    }

}

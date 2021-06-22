package com.n11.interview;

import junit.framework.TestCase;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Stream processing unit test of {@link N11Analytics}, using TopologyTestDriver.
 * <p>
 * See {@link N11Analytics} for further documentation.
 */

public class WordCountDemoTest extends TestCase {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    private final StringSerializer stringSerializer = new StringSerializer();
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        //testDriver = new TopologyTestDriver(builder.build(), N11Analytics.getStreamsConfig("localhost:9092"));
        inputTopic = testDriver.createInputTopic(N11Analytics.INPUT_TOPIC,
                stringSerializer,
                stringSerializer);
        outputTopic = testDriver.createOutputTopic(N11Analytics.OUTPUT_TOPIC_1,
                stringDeserializer,
                longDeserializer);
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    /**
     * Simple test validating ....
     */
    @Test
    public void testUsers() {
        //    users = [['ebe14efb-402d-4520-aa80-267aae5d8c8d','Ankara'],['62edd749-8cc1-478d-9fbe-86a6e8d3eae2','Izmir'],
        //            ['a10d419f-f582-4f79-b7bf-600a02df3b01','Istanbul'],['15e3422a-f80e-46be-bcb5-998782aa9133','Giresun']
        inputTopic.pipeInput("ebe14efb-402d-4520-aa80-267aae5d8c8d", Instant.ofEpochMilli(1L));
        //Read and validate output
        final KeyValue<String, Long> output = outputTopic.readKeyValue();
        assertThat(output, equalTo(KeyValue.pair("ebe14efb-402d-4520-aa80-267aae5d8c8d", 1L)));
        //No more output in topic
        assertTrue(outputTopic.isEmpty());
    }

    /**
     * Simple test validating ....
     */
    @Test
    public void testProducts() {
        //	products = [['f58a92be-8621-451b-b031-ed01a2d94a37',95],['8c0ba806-0184-41cd-87ca-6094ea8eec66',91],
        //	            ['91ccf030-cd0e-4195-b4b4-3c073ba4ca16',7],['9c5b2f88-c1da-40c0-b7be-83159aa4e570',48],
        inputTopic.pipeInput("f58a92be-8621-451b-b031-ed01a2d94a37", Instant.ofEpochMilli(1L));
        //Read and validate output
        final KeyValue<String, Long> output = outputTopic.readKeyValue();
        assertThat(output, equalTo(KeyValue.pair("f58a92be-8621-451b-b031-ed01a2d94a37", 1L)));
        //No more output in topic
        assertTrue(outputTopic.isEmpty());
    }

    /**
     * Simple test validating count of one word
     */
    @Test
    public void testOneWord() {
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello", Instant.ofEpochMilli(1L));
        //Read and validate output
        final KeyValue<String, Long> output = outputTopic.readKeyValue();
        assertThat(output, equalTo(KeyValue.pair("hello", 1L)));
        //No more output in topic
        assertTrue(outputTopic.isEmpty());
    }

    /**
     * Test Word count of sentence list.
     */
    @Test
    public void shouldCountWords() {
        final List<String> inputValues = Arrays.asList(
                "Hello Kafka Streams",
                "All streams lead to Kafka",
                "Join Kafka Summit",
                "И теперь пошли русские слова"
        );
        final Map<String, Long> expectedWordCounts = new HashMap<>();
        expectedWordCounts.put("hello", 1L);
        expectedWordCounts.put("all", 1L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("lead", 1L);
        expectedWordCounts.put("to", 1L);
        expectedWordCounts.put("join", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("summit", 1L);
        expectedWordCounts.put("и", 1L);
        expectedWordCounts.put("теперь", 1L);
        expectedWordCounts.put("пошли", 1L);
        expectedWordCounts.put("русские", 1L);
        expectedWordCounts.put("слова", 1L);

        inputTopic.pipeValueList(inputValues, Instant.ofEpochMilli(1L), Duration.ofMillis(100L));
        assertThat(outputTopic.readKeyValuesToMap(), equalTo(expectedWordCounts));
    }



    public void testGetStreamsConfig() {
    }

    public void testCalculateProductViewsInWindow() {
    }

    public void testCalculateUsersInWindow() {
    }

    public void testCalculateGMV() {
    }

    public void testCalculateTrendingTimeSlot() {
    }

    public void testMain() {

    }

}



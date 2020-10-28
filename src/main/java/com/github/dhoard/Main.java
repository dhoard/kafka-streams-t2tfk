package com.github.dhoard;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private final static Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "cp-6-0-x.address.cx:9092";

        String applicationId =
            Streams.class.getName().replace(".", "-");

        String stateDirConfig = "/tmp";
        String autoOffsetResetConfig = "earliest";
        String inputTopic1 = "input-topic-1";
        String inputTopic2 = "input-topic-2";
        String outputTopic = "output-topic";

        LOGGER.info("bootstrapServers      = [{}]", bootstrapServers);
        LOGGER.info("applicationId         = [{}]", applicationId);
        LOGGER.info("stateDirConfig        = [{}]", stateDirConfig);
        LOGGER.info("autoOffsetResetConfig = [{}]", autoOffsetResetConfig);
        LOGGER.info("inputTopic1           = [{}]", inputTopic1);
        LOGGER.info("inputTopic1           = [{}]", inputTopic2);
        LOGGER.info("outputTopic           = [{}]", outputTopic);

        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamProperties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirConfig);
        streamProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);

        streamProperties.put(
            "default.deserialization.exception.handler", LogAndContinueExceptionHandler.class);

        Properties additionalProperties = new Properties();
        additionalProperties.put("inputTopic1", inputTopic1);
        additionalProperties.put("inputTopic2", inputTopic2);
        additionalProperties.put("outputTopic", outputTopic);

        Streams streams = null;

        try {
            streams = new Streams(streamProperties, additionalProperties);
            streams.start();
            streams.await();
        } finally {
            streams.destroy();
        }
    }
}

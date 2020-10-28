package com.github.dhoard;

import com.github.dhoard.util.ThrowableUtil;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Streams {

    private final static Logger LOGGER = LoggerFactory.getLogger(Streams.class);

    public static final String BOOTSTRAP_SERVERS = "cp-6-0-x.address.cx:9092";

    public static final String APPLICATION_ID =
        Streams.class.getName().replace(".", "-");

    public static final String AUTO_OFFSET_RESET_CONFIG = "earliest";

    public static final String STATE_DIR_CONFIG = "/tmp";

    public static final  String INPUT_TOPIC_1 = "input-topic-1";

    public static final  String INPUT_TOPIC_2 = "input-topic-2";

    public static final  String OUTPUT_TOPIC = "output-topic";

    private Properties streamProperties;

    private Properties additionalProperties;

    private CountDownLatch countDownLatch;

    private StreamsBuilder streamsBuilder;

    private KafkaStreams kafkaStreams;

    private Serde<JSONObject> jsonObjectSerde;

    private KTable<String, JSONObject> inputAccountKTable;

    private KTable<String, JSONObject> inputCustomerDataKTable;

    private KTable<String, JSONObject> accountCustomerDataKTable;

    public Streams(Properties streamProperties, Properties additionalProperties) {
        this.streamProperties = streamProperties;
        this.additionalProperties = additionalProperties;
    }

    public void start() throws Exception {
        this.countDownLatch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            this.countDownLatch.countDown();
        }));

        this.streamsBuilder = new StreamsBuilder();

        this.jsonObjectSerde =
            Serdes.serdeFrom(
                new JSONObjectSerializer(),
                new JSONObjectDeserializer());

        this.inputAccountKTable =
            this.streamsBuilder.table(
                this.additionalProperties.getProperty("inputTopic1"),
                Consumed.with(Serdes.String(), this.jsonObjectSerde));

        this.inputCustomerDataKTable =
            this.streamsBuilder.table(
                this.additionalProperties.getProperty("inputTopic2"),
                Consumed.with(Serdes.String(), this.jsonObjectSerde));

        this.accountCustomerDataKTable =
            this.inputAccountKTable.join(
                this.inputCustomerDataKTable,
                new ExtractFunction(),
                new CustomValueJoiner());

        this.accountCustomerDataKTable.toStream().to(
            this.additionalProperties.getProperty("outputTopic"),
            Produced.with(Serdes.String(), this.jsonObjectSerde));

        this.kafkaStreams = new KafkaStreams(this.streamsBuilder.build(), this.streamProperties);
        this.kafkaStreams.setUncaughtExceptionHandler((t, e) -> {
            this.countDownLatch.countDown();
        });

        this.kafkaStreams.start();
    }

    public void await() {
        if (null != this.countDownLatch) {
            try {
                this.countDownLatch.await();
            } catch (Throwable t) {
                ThrowableUtil.throwUnchecked(t);
            }
        }
    }

    public void destroy() {
        if (null != this.countDownLatch) {
            this.countDownLatch.countDown();
        }

        if (null != this.kafkaStreams) {
            try {
                this.kafkaStreams.close();
            } catch (Throwable t) {
                LOGGER.error("Exception closing Kafka streams", t);
            }

            this.kafkaStreams = null;
        }
    }

    class ExtractFunction implements Function<JSONObject, String> {

        @Override
        public String apply(JSONObject accountJSONObject) {
            return accountJSONObject.getString("CUSTOMER_NO");
        }
    }

    class CustomValueJoiner implements ValueJoiner<JSONObject, JSONObject, JSONObject> {

        @Override
        public JSONObject apply(JSONObject accountJSONObject, JSONObject customerData) {
            JSONObject resultJSONObject = new JSONObject();

            for (String key : accountJSONObject.keySet()) {
                resultJSONObject.put(key, accountJSONObject.getString(key));
            }

            for (String key : customerData.keySet()) {
                resultJSONObject.put(key, customerData.getString(key));
            }

            return resultJSONObject;
        }
    }
}

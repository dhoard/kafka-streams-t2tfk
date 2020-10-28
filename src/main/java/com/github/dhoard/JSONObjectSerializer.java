package com.github.dhoard;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;

public class JSONObjectSerializer implements Serializer<JSONObject> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // DO NOTHING
    }

    @Override
    public byte[] serialize(String topic, JSONObject JSONObject) {
        return JSONObject.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // DO NOTHING
    }
}

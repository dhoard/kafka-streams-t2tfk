package com.github.dhoard;

import com.github.dhoard.util.JSONUtil;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONObject;

public class JSONObjectDeserializer implements Deserializer<JSONObject> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // DO NOTHING
    }

    @Override
    public JSONObject deserialize(String s, byte[] bytes) {
        return JSONUtil.parseJSONObject(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        // DO NOTHING
    }
}

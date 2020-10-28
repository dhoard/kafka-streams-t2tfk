package com.github.dhoard;

import com.github.dhoard.util.JSONUtil;
import junit.framework.TestCase;
import org.json.JSONObject;

public class JSONUtilTestCase extends TestCase {

    public void test1() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key", "value");

        String json = JSONUtil.toJSON(jsonObject);
        System.out.println(json);
    }

    public void test2() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key", "value");

        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("subKey", "subValue");
        jsonObject2.put("a", "a value");

        jsonObject.put("nestedKey", jsonObject2);

        String json = JSONUtil.toJSON(jsonObject, false);
        System.out.println(json);

        System.out.println();

        json = JSONUtil.toJSON(jsonObject, false);
        System.out.println(json);
    }

    public void test3() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key", "value");

        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("subKey", "subValue");
        jsonObject2.put("a", "a value");

        jsonObject.put("nestedKey", jsonObject2);

        JSONObject jsonObject3 = new JSONObject();
        jsonObject3.put("AA", "BB");

        jsonObject2.put("c sub sub", jsonObject3);

        String json = JSONUtil.toJSON(jsonObject, true);
        System.out.println(json);
    }

    public void test4() {
        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("A", "A");
        jsonObject1.put("Z", "Z");
        jsonObject1.put("1", "1");

        String json = JSONUtil.toJSON(jsonObject1, true);
        System.out.println(json);

        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("1", "1");
        jsonObject2.put("Z", "Z");
        jsonObject2.put("A", "A");

        json = JSONUtil.toJSON(jsonObject2, true);
        System.out.println(json);

        System.out.println("equals  = " + jsonObject1.equals(jsonObject2));
        System.out.println("similar = " + jsonObject1.similar(jsonObject2));
    }
}

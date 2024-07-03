package com.zilliz.milvustestv2.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.HashMap;
import java.util.Map;

public class JsonObjectUtil {

    public static JsonObject jsonMerge(JsonObject jsonObject1,JsonObject jsonObject2){
        Gson gson = new Gson();
        // 将 JsonObject 转换为 Map
        Map<String, Object> map1 = gson.fromJson(jsonObject1, HashMap.class);
        Map<String, Object> map2 = gson.fromJson(jsonObject2, HashMap.class);

        // 合并两个 Map
        Map<String, Object> mergedMap = new HashMap<>(map1);
        mergedMap.putAll(map2);

        // 将合并后的 Map 转换为 JsonObject
        JsonObject mergedJsonObject = JsonParser.parseString(gson.toJson(mergedMap)).getAsJsonObject();
        return mergedJsonObject;
    }
}

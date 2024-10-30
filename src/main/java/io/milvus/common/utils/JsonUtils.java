/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.common.utils;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

public class JsonUtils {
    // Set ToNumberPolicy.LONG_OR_DOUBLE so that integer can be parsed as integer, float be parsed as float.
    // Gson doc declared "Gson instances are Thread-safe so you can reuse them freely across multiple threads."
    // So we can use it as a global static instance.
    // https://www.javadoc.io/doc/com.google.code.gson/gson/2.10.1/com.google.gson/com/google/gson/Gson.html
    private static final Gson GSON_INSTANCE = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();

    public static <T> T fromJson(String jsonStr, Class<T> classOfT) {
        return GSON_INSTANCE.fromJson(jsonStr, classOfT);
    }

    public static <T> T fromJson(String jsonStr, Type typeOfT) {
        return GSON_INSTANCE.fromJson(jsonStr, typeOfT);
    }

    public static <T> T fromJson(String jsonStr, TypeToken<T> typeOfT) {
        return GSON_INSTANCE.fromJson(jsonStr, typeOfT);
    }


    public static <T> T fromJson(JsonElement jsonElement, Class<T> classOfT) {
        return GSON_INSTANCE.fromJson(jsonElement, classOfT);
    }

    public static <T> T fromJson(JsonElement jsonElement, Type typeOfT) {
        return GSON_INSTANCE.fromJson(jsonElement, typeOfT);
    }

    public static <T> T fromJson(JsonElement jsonElement, TypeToken<T> typeOfT) {
        return GSON_INSTANCE.fromJson(jsonElement, typeOfT);
    }

    public static String toJson(Object obj) {
        return GSON_INSTANCE.toJson(obj);
    }

    public static String toJson(JsonElement jsonElement) {
        return GSON_INSTANCE.toJson(jsonElement);
    }

    public static <T> JsonElement toJsonTree(T obj) {
        return GSON_INSTANCE.toJsonTree(obj);
    }
}

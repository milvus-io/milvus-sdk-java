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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * JSON serialization and deserialization helpers backed by a thread-safe Gson instance.
 *
 * <p>The underlying Gson is configured with the {@code LONG_OR_DOUBLE} number policy so that
 * integer values are parsed as {@code long} and float values as {@code double}.
 */
public class JsonUtils {
    // Set ToNumberPolicy.LONG_OR_DOUBLE so that integer can be parsed as integer, float be parsed as float.
    // Gson doc declared "Gson instances are Thread-safe so you can reuse them freely across multiple threads."
    // So we can use it as a global static instance.
    // https://www.javadoc.io/doc/com.google.code.gson/gson/2.10.1/com.google.gson/com/google/gson/Gson.html
    private static final Gson GSON_INSTANCE = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .create();

    /**
     * Deserializes the given JSON string into an object of the specified class.
     *
     * @param jsonStr the JSON string to parse
     * @param classOfT the class of the target object
     * @return the deserialized object
     */
    public static <T> T fromJson(String jsonStr, Class<T> classOfT) {
        return GSON_INSTANCE.fromJson(jsonStr, classOfT);
    }

    /**
     * Deserializes the given JSON string into an object of the specified generic type.
     *
     * @param jsonStr the JSON string to parse
     * @param typeOfT the generic type of the target object
     * @return the deserialized object
     */
    public static <T> T fromJson(String jsonStr, Type typeOfT) {
        return GSON_INSTANCE.fromJson(jsonStr, typeOfT);
    }

    /**
     * Deserializes the given JSON string into an object of the specified type token.
     *
     * @param jsonStr the JSON string to parse
     * @param typeOfT the type token of the target object
     * @return the deserialized object
     */
    public static <T> T fromJson(String jsonStr, TypeToken<T> typeOfT) {
        return GSON_INSTANCE.fromJson(jsonStr, typeOfT);
    }


    /**
     * Deserializes the given JSON element into an object of the specified class.
     *
     * @param jsonElement the JSON element to parse
     * @param classOfT the class of the target object
     * @return the deserialized object
     */
    public static <T> T fromJson(JsonElement jsonElement, Class<T> classOfT) {
        return GSON_INSTANCE.fromJson(jsonElement, classOfT);
    }

    /**
     * Deserializes the given JSON element into an object of the specified generic type.
     *
     * @param jsonElement the JSON element to parse
     * @param typeOfT the generic type of the target object
     * @return the deserialized object
     */
    public static <T> T fromJson(JsonElement jsonElement, Type typeOfT) {
        return GSON_INSTANCE.fromJson(jsonElement, typeOfT);
    }

    /**
     * Deserializes the given JSON element into an object of the specified type token.
     *
     * @param jsonElement the JSON element to parse
     * @param typeOfT the type token of the target object
     * @return the deserialized object
     */
    public static <T> T fromJson(JsonElement jsonElement, TypeToken<T> typeOfT) {
        return GSON_INSTANCE.fromJson(jsonElement, typeOfT);
    }

    /**
     * Serializes the given object into a JSON string.
     *
     * @param obj the object to serialize
     * @return the JSON string
     */
    public static String toJson(Object obj) {
        return GSON_INSTANCE.toJson(obj);
    }

    /**
     * Serializes the given JSON element into a JSON string.
     *
     * @param jsonElement the JSON element to serialize
     * @return the JSON string
     */
    public static String toJson(JsonElement jsonElement) {
        return GSON_INSTANCE.toJson(jsonElement);
    }

    /**
     * Converts the given object into a {@link JsonElement} tree.
     *
     * @param obj the object to convert
     * @return the JSON element tree
     */
    public static <T> JsonElement toJsonTree(T obj) {
        return GSON_INSTANCE.toJsonTree(obj);
    }

    /**
     * Parses the given JSON string into a {@link JsonObject}.
     *
     * <p>An empty or {@code null} string is parsed as an empty {@code JsonObject}.
     *
     * @param jsonStr the JSON string to parse
     * @return the parsed JSON object
     */
    public static JsonObject parseFromString(String jsonStr) {
        if (jsonStr == null || jsonStr.isEmpty()) {
            return new JsonObject();
        }
        return JsonParser.parseString(jsonStr).getAsJsonObject();
    }

    /**
     * Serializes the given {@link JsonObject} into a JSON string.
     *
     * <p>A {@code null} object is serialized as an empty string.
     *
     * @param jsonObject the JSON object to serialize
     * @return the JSON string
     */
    public static String toJsonString(JsonObject jsonObject) {
        return jsonObject != null ? jsonObject.toString() : "";
    }
}

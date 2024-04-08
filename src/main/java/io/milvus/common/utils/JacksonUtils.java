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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonUtils {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static <T> T fromJson(String jsonStr, TypeReference<T> typeRef) {
        try {
            return objectMapper.readValue(jsonStr, typeRef);
        } catch (Exception e) {
            throw new IllegalArgumentException("json deserialization error, e=", e);
        }
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        try {
            return objectMapper.readValue(jsonStr, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("json deserialization error, e=", e);
        }
    }

    public static <T> T fromJson(byte[] bytes, TypeReference<T> typeRef) {
        try {
            return objectMapper.readValue(bytes, typeRef);
        } catch (Exception e) {
            throw new IllegalArgumentException("json deserialization error, e=", e);
        }
    }

    public static <T> T fromJson(byte[] bytes, Class<T> type) {
        try {
            return objectMapper.readValue(bytes, type);
        } catch (Exception e) {
            throw new IllegalArgumentException("json deserialization error, e=", e);
        }
    }

    public static String toJsonString(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new IllegalArgumentException("json serialization error, e=", e);
        }
    }

    public static byte[] toJsonByte(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new IllegalArgumentException("json serialization error, e=", e);
        }
    }

}
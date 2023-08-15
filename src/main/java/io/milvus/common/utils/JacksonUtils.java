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
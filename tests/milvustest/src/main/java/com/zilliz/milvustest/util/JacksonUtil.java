package com.zilliz.milvustest.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonUtil {

  public static <T> T deserialize(String jsonStr, Class<T> clazz) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(jsonStr, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String serialize(Object obj) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String formatJson(String json) {
    String SPACE = "   ";
    StringBuffer result = new StringBuffer();
    int length = json.length();
    int number = 0;
    char key = 0;
    for (int i = 0; i < length; i++) {
      key = json.charAt(i);
      if ((key == '[') || (key == '{')) {

        if ((i - 1 > 0) && (json.charAt(i - 1) == ':')) {
          result.append('\n');
          result.append(indent(number));
        }
        result.append(key);
        result.append('\n');
        number++;
        result.append(indent(number));
        continue;
      }
      if ((key == ']') || (key == '}')) {
        result.append('\n');
        number--;
        result.append(indent(number));
        result.append(key);
        if (((i + 1) < length) && (json.charAt(i + 1) != ',')) {
          result.append('\n');
        }
        continue;
      }
      if ((key == ',')) {
        result.append(key);
        result.append('\n');
        result.append(indent(number));
        continue;
      }
      result.append(key);
    }
    return result.toString();
  }

  private static String indent(int number) {
    String SPACE = "   ";
    StringBuffer result = new StringBuffer();
    for (int i = 0; i < number; i++) {
      result.append(SPACE);
    }
    return result.toString();
  }
}

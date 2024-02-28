package com.zilliz.milvustestv2.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

public class PropertyFilesUtil {
  static Logger logger = LoggerFactory.getLogger(PropertyFilesUtil.class);

  public static HashMap<String, String> readPropertyFile(String propertyFileName) {
    HashMap<String, String> hashMap = new HashMap<>();
    Properties prop = new Properties();
    try {
      InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(propertyFileName)));
      prop.load(in); // /加载属性列表
      for (String key : prop.stringPropertyNames()) {
        hashMap.put(key, prop.getProperty(key));
      }
      in.close();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    return hashMap;
  }

  public static String getRunValue(String key) {
    HashMap<String, String> hashMap =
        PropertyFilesUtil.readPropertyFile("./src/test/resources/run.properties");
    String value = "";
    value = hashMap.get(key);
    return value;
  }
}

package com.zilliz.milvustest.util;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

public class PropertyFilesUtil {
  static Logger logger = Logger.getLogger(PropertyFilesUtil.class);

  public static HashMap<String, String> readPropertyFile(String propertyFileName) {
    HashMap<String, String> hashMap = new HashMap<>();
    Properties prop = new Properties();
    try {
      InputStream in = new BufferedInputStream(new FileInputStream(propertyFileName));
      prop.load(in); // /加载属性列表
      Iterator<String> it = prop.stringPropertyNames().iterator();
      while (it.hasNext()) {
        String key = it.next();
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
        PropertyFilesUtil.readPropertyFile("./src/test/java/resources/run.properties");
    String value = "";
    value = hashMap.get(key);
    return value;
  }
}

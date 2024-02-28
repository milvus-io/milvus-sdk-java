package com.zilliz.milvustestv2.utils;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class FileUtils {
  public static Logger logger = LoggerFactory.getLogger(FileUtils.class);

  // generate Json File
  public static boolean createFile(
      String jsonStr, String filePath, String fileName, String fileType) {
    boolean flag = true;
    String fullPath = filePath + File.separator + fileName + "." + fileType;
    File file = new File(fullPath);
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
      Writer writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
      writer.write(jsonStr);
      writer.flush();
      writer.close();
    } catch (IOException e) {
      logger.error(e.getMessage());
      flag = false;
    }
    return flag;
  }


  //把一个文件中的内容读取成一个String字符串
  public static String getStr(File jsonFile){
    String jsonStr = "";
    try {
      FileReader fileReader = new FileReader(jsonFile);
      Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
      int ch = 0;
      StringBuffer sb = new StringBuffer();
      while ((ch = reader.read()) != -1) {
        sb.append((char) ch);
      }
      fileReader.close();
      reader.close();
      jsonStr = sb.toString();
      return jsonStr;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

}

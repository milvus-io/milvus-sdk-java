package com.zilliz.milvustestv2.utils;


import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
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

  public static void multiFilesUpload(String path, List<String> fileNameList, String bucketFolder)
          throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    if (bucketFolder == null) bucketFolder = "";
    try {
      // Create a minioClient with the MinIO server playground, its access key and secret key.
      MinioClient minioClient =
              MinioClient.builder()
                      .endpoint(PropertyFilesUtil.getRunValue("minio"))
                      .credentials(
                              PropertyFilesUtil.getRunValue("accesskey"),
                              PropertyFilesUtil.getRunValue("secretkey"))
                      .build();

      // Make 'jsonBucket' bucket if not exist.
      boolean found =
              minioClient.bucketExists(BucketExistsArgs.builder().bucket("milvus-bucket").build());
      if (!found) {
        // Make a new bucket called 'jsonBucket'.
        minioClient.makeBucket(MakeBucketArgs.builder().bucket("milvus-bucket").build());
      } else {
        System.out.println("Bucket 'milvus-bucket' already exists.");
      }

      for (String fileName : fileNameList) {
        minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket("milvus-bucket")
                        .object(bucketFolder + "/" + fileName)
                        .filename(path + fileName)
                        .build());
        System.out.println(
                "'"
                        + path
                        + fileName
                        + "' is successfully uploaded as "
                        + "object '"
                        + fileName
                        + "' to bucket 'milvus-bucket'.");
      }
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
      System.out.println("HTTP trace: " + e.httpTrace());
    }
  }
}

package com.zilliz.milvustest.util;

import com.zilliz.milvustest.entity.FieldType;
import com.zilliz.milvustest.entity.FileBody;
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
  // generateJsonBody

  public static String generateFileBody(
      Boolean rowBased,
      int rows,
      int dim,
      List<FileBody> fileBodyList,
      Boolean autoPK,
      int fileNumber) {
    // init filed value
    if (autoPK == null) {
      autoPK = true;
    }
    Boolean finalAutoPK = autoPK;
    fileBodyList.stream()
        .forEach(
            x -> {
              if (x.getFieldType().equals(FieldType.PK_FIELD)) {
                Integer[] ints = MathUtil.generateIntPK(rows, finalAutoPK, fileNumber);
                List<Integer> integerList = Arrays.asList(ints);
                x.setFieldValue(integerList);
              }
              if (x.getFieldType().equals(FieldType.INT_FIELD)) {
                Integer[] ints = MathUtil.generateInt(rows, true);
                List<Integer> integerList = Arrays.asList(ints);
                x.setFieldValue(integerList);
              }
              if (x.getFieldType().equals(FieldType.FLOAT_FIELD)) {
                Float[] floats = MathUtil.generateFloat(rows);
                x.setFieldValue(Arrays.asList(floats));
              }
              if (x.getFieldType().equals(FieldType.STRING_FIELD)) {
                String[] strings = MathUtil.generateString(rows);
                x.setFieldValue(Arrays.asList(strings));
              }
              if (x.getFieldType().equals(FieldType.BOOLEAN_FIELD)) {
                Boolean[] booleans = MathUtil.generateBoolean(rows);
                x.setFieldValue(Arrays.asList(booleans));
              }
              if (x.getFieldType().equals(FieldType.LONG_FIELD)) {
                Long[] longs = MathUtil.generateLong(rows);
                x.setFieldValue(Arrays.asList(longs));
              }
              if (x.getFieldType().equals(FieldType.FLOAT_VECTOR_FIELD)) {
                List<Double[]> doubesList = MathUtil.generateFloatVector(rows, 20, dim);
                x.setFieldValue(doubesList);
              }
              if (x.getFieldType().equals(FieldType.STRING_PK_FIELD)) {
                String[] strings = MathUtil.generateString(rows);
                x.setFieldValue(Arrays.asList(strings));
              }
            });
    if (rowBased) {

      List<Map> rowsList = new ArrayList<>();
      for (int r = 0; r < rows; r++) {
        Map map = new HashMap();
        for (int i = 0; i < fileBodyList.size(); i++) {
          map.put(fileBodyList.get(i).getFieldName(), fileBodyList.get(i).getFieldValue().get(r));
        }
        rowsList.add(map);
      }
      HashMap rowsMap = new HashMap();
      rowsMap.put("rows", rowsList);
      return JacksonUtil.serialize(rowsMap);
    } else {
      HashMap map = new HashMap();
      fileBodyList.stream()
          .forEach(
              x -> {
                map.put(x.getFieldName(), x.getFieldValue());
              });
      return JacksonUtil.serialize(map);
    }
  }

  /**
   * @param rowBased: true-rows_file,false-column_file,
   * @param rows: the numbers of entities to be generated in the file
   * @param dim: dim of the vector data
   * @param fileBodyList: entity of FileBody
   * @param autoPK: generate PK sequence or not
   * @param filePath: specify file path
   * @param fileName: specify file name
   * @param fileType: specify the file suffix
   * @param fileNums: generate the number of files
   * @return
   */
  public static Boolean generateMultipleFiles(
      Boolean rowBased,
      int rows,
      int dim,
      List<FileBody> fileBodyList,
      Boolean autoPK,
      String filePath,
      String fileName,
      String fileType,
      int fileNums) {
    Boolean resultFlag = true;
    for (int i = 0; i < fileNums; i++) {
      String s = generateFileBody(rowBased, rows, dim, fileBodyList, autoPK, i + 1);
      s = JacksonUtil.formatJson(s);
      boolean file = createFile(s, filePath, fileName + i, fileType);
      if (!file) {
        resultFlag = false;
      }
    }
    return resultFlag;
  }

  public static void fileUploader(String path, String fileName, String bucketFolder)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    if (bucketFolder == null) bucketFolder = "";
    try {
      // Create a minioClient with the MinIO server playground, its access key and secret key.
      MinioClient minioClient =
          MinioClient.builder()
              .endpoint(PropertyFilesUtil.getRunValue("minioHost"))
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

      // Upload './src/test/java/resources/autojson/jsonData0.json' as object name 'jsonData0.json'
      // to bucket
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
    } catch (MinioException e) {
      System.out.println("Error occurred: " + e);
      System.out.println("HTTP trace: " + e.httpTrace());
    }
  }

  public static void multiFilesUpload(String path, List<String> fileNameList, String bucketFolder)
      throws IOException, NoSuchAlgorithmException, InvalidKeyException {
    if (bucketFolder == null) bucketFolder = "";
    try {
      // Create a minioClient with the MinIO server playground, its access key and secret key.
      MinioClient minioClient =
          MinioClient.builder()
              .endpoint(PropertyFilesUtil.getRunValue("minioHost"))
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

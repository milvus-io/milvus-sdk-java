package com.zilliz.milvustest.limit;

import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.FileUtils;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.bulkinsert.GetBulkInsertStateParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author yongpeng.li @Date 2022/10/11 11:44
 */
@Slf4j
public class DMLTest {
  public static final MilvusServiceClient milvusClient =
              new MilvusServiceClient(
                      ConnectParam.newBuilder()
                              .withUri(
                                      System.getProperty("milvusUri") == null
                                              ? PropertyFilesUtil.getRunValue("milvusUri")
                                              : System.getProperty("milvusUri"))
                              .withAuthorization("db_admin", "P@ssw0rd")
                              .build());

  @Test(description = "InsertRateTest")
  public void InsertRateTest() {
    R<GetCollectionStatisticsResponse> autoindex_l2 = milvusClient.getCollectionStatistics(GetCollectionStatisticsParam.newBuilder()
            .withFlush(true).withCollectionName("autoindex_l2").build());
    System.out.println(autoindex_l2);
    String collectionName = "book128";
    int dim = 128;
    FieldType bookIdField =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType wordCountField =
        FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType bookIntroField =
        FieldType.newBuilder()
            .withName(CommonData.defaultVectorField)
            .withDataType(DataType.FloatVector)
            .withDimension(dim)
            .build();
    CreateCollectionParam createCollectionParam =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDescription("Test book search")
            .withShardsNum(2)
            .addFieldType(bookIdField)
            .addFieldType(wordCountField)
            .addFieldType(bookIntroField)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
    log.info(collection.toString());
    // insert data with customized ids
    Random ran = new Random();
    int singleNum = 1000;
    int insertRounds = 2000;
    double insertTotalTime = 0.00;
    for (int r = 0; r < insertRounds; r++) {
      List<Long> book_id_array = new ArrayList<>();
      List<Long> word_count_array = new ArrayList<>();
      List<List<Float>> book_intro_array = new ArrayList<>();
      for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
        book_id_array.add(i);
        word_count_array.add(i + 10000);
        List<Float> vector = new ArrayList<>();
        for (int k = 0; k < dim; ++k) {
          vector.add(ran.nextFloat());
        }
        book_intro_array.add(vector);
      }
      List<InsertParam.Field> fields = new ArrayList<>();
      fields.add(new InsertParam.Field(bookIdField.getName(), book_id_array));
      fields.add(new InsertParam.Field(wordCountField.getName(), word_count_array));
      fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
      InsertParam insertParam =
          InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build();
      long startTime = System.currentTimeMillis();
      R<MutationResult> insertR = milvusClient.insert(insertParam);
      log.info(insertR.getStatus().toString());
      long endTime = System.currentTimeMillis();
      insertTotalTime = (endTime - startTime) / 1000.00;
      log.info(
          "--NO."
              + r
              + "----- insert "
              + singleNum
              + " entities cost "
              + insertTotalTime
              + " seconds");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test(description = "InsertRateTest")
  public void InsertRateTest1() {

    String collectionName = "book128";
    int dim = 128;
    FieldType bookIdField =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType wordCountField =
        FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType bookIntroField =
        FieldType.newBuilder()
            .withName(CommonData.defaultVectorField)
            .withDataType(DataType.FloatVector)
            .withDimension(dim)
            .build();
    CreateCollectionParam createCollectionParam =
        CreateCollectionParam.newBuilder()
            .withCollectionName(collectionName)
            .withDescription("Test book search")
            .withShardsNum(2)
            .addFieldType(bookIdField)
            .addFieldType(wordCountField)
            .addFieldType(bookIntroField)
            .build();
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
    log.info(collection.toString());
    // insert data with customized ids
    Random ran = new Random();
    int singleNum = 10000;
    int insertRounds = 300;
    double insertTotalTime = 0.00;
    for (int r = 0; r < insertRounds; r++) {
      List<Long> book_id_array = new ArrayList<>();
      List<Long> word_count_array = new ArrayList<>();
      List<List<Float>> book_intro_array = new ArrayList<>();
      for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
        book_id_array.add(i);
        word_count_array.add(i + 10000);
        List<Float> vector = new ArrayList<>();
        for (int k = 0; k < dim; ++k) {
          vector.add(ran.nextFloat());
        }
        book_intro_array.add(vector);
      }
      List<InsertParam.Field> fields = new ArrayList<>();
      fields.add(new InsertParam.Field(bookIdField.getName(), book_id_array));
      fields.add(new InsertParam.Field(wordCountField.getName(), word_count_array));
      fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
      InsertParam insertParam =
          InsertParam.newBuilder().withCollectionName(collectionName).withFields(fields).build();
      long startTime = System.currentTimeMillis();
      R<MutationResult> insertR = milvusClient.insert(insertParam);
      log.info(insertR.getStatus().toString());
      long endTime = System.currentTimeMillis();
      insertTotalTime = (endTime - startTime) / 1000.00;
      log.info("------ insert " + singleNum + " entities cost " + insertTotalTime + " seconds");
    }
  }

  @Test(description = "DeleteRateTest")
  public void DeleteRateTest() {
    int insertRounds = 10000;
    List<Long> book_id_array = new ArrayList<>();
    for (long i = 0; i < 10000; ++i) {
      book_id_array.add(i);
    }
    for (int r = 0; r < insertRounds; r++) {

      R<MutationResult> delete =
          milvusClient.delete(
              DeleteParam.newBuilder()
                  .withCollectionName("book128")
                  .withExpr("book_id in " + book_id_array)
                  .build());
      log.info("第" + r + "次," + delete.getStatus().toString());
    }
  }
  @Test(description = "DeleteRateTest")
  public void DeleteRateTest2() {
    int insertRounds = 10000;
    List<Long> book_id_array = new ArrayList<>();
    for (long i = 0; i < 10000; ++i) {
      book_id_array.add(i);
    }
    for (int r = 0; r < insertRounds; r++) {

      R<MutationResult> delete =
              milvusClient.delete(
                      DeleteParam.newBuilder()
                              .withCollectionName("book128")
                              .withExpr("book_id in " + book_id_array)
                              .build());
      log.info("第" + r + "次," + delete.getStatus().toString());
    }
  }
  @Test(description = "DeleteRateTest")
  public void DeleteRateTest3() {
    int insertRounds = 10000;
    List<Long> book_id_array = new ArrayList<>();
    for (long i = 0; i < 10000; ++i) {
      book_id_array.add(i);
    }
    for (int r = 0; r < insertRounds; r++) {

      R<MutationResult> delete =
              milvusClient.delete(
                      DeleteParam.newBuilder()
                              .withCollectionName("book128")
                              .withExpr("book_id in " + book_id_array)
                              .build());
      log.info("第" + r + "次," + delete.getStatus().toString());
    }
  }

  @Test(description = "BulkLoadTest")
  public void BulkLoadTest()
      throws IOException, NoSuchAlgorithmException, InvalidKeyException, InterruptedException {
    R<ImportResponse> importResponseR =
        milvusClient.bulkInsert(
            BulkInsertParam.newBuilder()
                .withCollectionName("book128")
                .withFiles(
                    Arrays.asList("rowJson0.json"))
                .build());
    log.info(importResponseR.toString());
    Long taskId = importResponseR.getData().getTasks(0);
    for (int i = 0; i < 10; i++) {
      R<GetImportStateResponse> bulkloadState =
          milvusClient.getBulkInsertState(GetBulkInsertStateParam.newBuilder().withTask(taskId).build());
      log.info(bulkloadState.toString());
      Thread.sleep(1000);
    }
  }

}

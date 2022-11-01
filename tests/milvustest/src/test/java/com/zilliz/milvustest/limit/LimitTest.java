package com.zilliz.milvustest.limit;

import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import io.qameta.allure.Epic;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author yongpeng.li @Date 2022/9/28 15:03
 */
@Slf4j
@Epic("Limit")
public class LimitTest {
  public static final MilvusServiceClient milvusClient =
      new MilvusServiceClient(
          ConnectParam.newBuilder()
              .withHost(
                  System.getProperty("milvusHost") == null
                      ? PropertyFilesUtil.getRunValue("milvusHost")
                      : System.getProperty("milvusHost"))
              .withPort(
                  Integer.parseInt(
                      System.getProperty("milvusPort") == null
                          ? PropertyFilesUtil.getRunValue("milvusPort")
                          : System.getProperty("milvusPort")))
              .build());




  @Test(description = "describe collection test")
  public void describeCollectionTest() {
    List<String> collectionList = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      R<DescribeCollectionResponse> book =
          milvusClient.describeCollection(
              DescribeCollectionParam.newBuilder().withCollectionName("book").build());
      System.out.println("DescribeCollection result status:" + book.getStatus().toString());
      /* String newCollection = CommonFunction.createNewCollection();
      Thread.sleep(6000);
      R<Boolean> booleanR = milvusClient.hasCollection(HasCollectionParam.newBuilder().withCollectionName(newCollection).build());
      Thread.sleep(6000);
      System.out.println(booleanR);
      collectionList.add(newCollection);*/
    }
    /*  System.out.println("create collection:" + collectionList.size());
    R<ShowCollectionsResponse> showCollectionsResponseR =
        milvusClient.showCollections(
            ShowCollectionsParam.newBuilder()
                .withCollectionNames(collectionList)
                .build());
    System.out.println(showCollectionsResponseR.getData());
    for (int i = 0; i < 20; i++) {
      milvusClient.dropCollection(
          DropCollectionParam.newBuilder().withCollectionName(collectionList.get(i)).build());
    }*/
  }

  @Test(description = "insert limit")
  public void insertLimitRateTest() {
    String collectionName = "book256";
    int dim = 256;
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
            .withName("book_intro")
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
    milvusClient.createCollection(createCollectionParam);
    System.out.println("create collection " + collectionName + " successfully");
    // insert data with customized ids
    Random ran = new Random();
    int singleNum = 10000;
    int insertRounds = 200;
    long insertTotalTime = 0L;
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
      long endTime = System.currentTimeMillis();
      insertTotalTime += (endTime - startTime) / 1000.0;
    }
    System.out.println(
        "totally insert "
            + singleNum * insertRounds
            + " entities cost "
            + insertTotalTime
            + " seconds");
  }

  @Test(description = "Insert limit test2")
  public void insertLimitTest2() {
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
            .withName("book_intro")
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
    milvusClient.createCollection(createCollectionParam);
    System.out.println("create collection " + collectionName + " successfully");
    // insert data
    Random ran = new Random();
    int singleNum = 10000;
    int insertRounds = 300;
    long insertTotalTime = 0L;
    List<Long> book_id_array = new ArrayList<>();
    List<Long> word_count_array = new ArrayList<>();
    List<List<Float>> book_intro_array = new ArrayList<>();
    for (long i = 0; i < singleNum; ++i) {
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
    for (int r = 0; r < insertRounds; r++) {
      long startTime = System.currentTimeMillis();
      R<MutationResult> insertR = milvusClient.insert(insertParam);
      long endTime = System.currentTimeMillis();
      insertTotalTime = (long) ((endTime - startTime) / 1000.0);
      System.out.println(
          "** insert " + singleNum + " entities cost " + insertTotalTime + " seconds");
    }
  }

  @Test(description = "Concurrent insert limit ")
  public void insertLimitRateTestConcurrency() {
    String newCollection = CommonFunction.createNewCollection();
    System.out.println("create collection " + newCollection + " successfully");
    int poolNum=2;
    ExecutorService executorService = Executors.newFixedThreadPool(poolNum);
    int singleNum = 10000;
    int insertRounds = 100;

    List<InsertParam.Field> fields = CommonFunction.generateData(singleNum);
    InsertParam insertParam =
        InsertParam.newBuilder().withCollectionName(newCollection).withFields(fields).build();
    for (int e = 0; e < poolNum; e++) {
      int finalE = e;
      executorService.execute(
          () -> {
            for (int r = 0; r < insertRounds; r++) {
              long insertTotalTime = 0L;
              long startTime = System.currentTimeMillis();
              R<MutationResult> insertR = milvusClient.insert(insertParam);
              long endTime = System.currentTimeMillis();
              insertTotalTime = (long) ((endTime - startTime) / 1000.0);
              System.out.println(
                  "Thread "
                      + finalE
                      + " insert "
                      + singleNum
                      + " entities cost "
                      + insertTotalTime
                      + " seconds");
            }
          });
    }
    executorService.shutdown();
    try {
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.warn(e.getMessage());
    }
  }
}

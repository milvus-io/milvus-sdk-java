package com.zilliz.milvustest.limit;

import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.InsertParam;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Author yongpeng.li @Date 2022/10/12 17:20
 */
@Slf4j
public class BackPressure {
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

  @Test(description = "InsertRateTest")
  public void InsertRateTest() {

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
    R<RpcStatus> collection = milvusClient.createCollection(createCollectionParam);
    log.info(collection.toString());
    // insert data with customized ids
    Random ran = new Random();
    int singleNum = 10000;
    int insertRounds = 100;
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
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

package com.zilliz.milvustest.limit;

import com.zilliz.milvustest.util.MathUtil;
import com.zilliz.milvustest.util.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author yongpeng.li @Date 2022/10/12 09:45
 */
@Slf4j
public class DQLTest {
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

  @Test(description = "SearchRateTest")
  public void SearchRateTest() {
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");

    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName("book128").build());
    double searchTime = 0.00;
    for (int i = 0; i < 1000; i++) {
      List<List<Float>> search_vectors = new ArrayList<>();
      for (int k = 0; k < 3000; k++) {
        search_vectors.add(Arrays.asList(MathUtil.generateFloat(128)));
      }
      long startTime = System.currentTimeMillis();
      R<SearchResults> search =
          milvusClient.search(
              SearchParam.newBuilder()
                  .withCollectionName("book128")
                  .withMetricType(MetricType.L2)
                  .withTopK(2)
                  .withParams(SEARCH_PARAM)
                  .withVectors(search_vectors)
                  .withOutFields(search_output_fields)
                  .withVectorFieldName("book_intro")
                      .withExpr("book_id>10")
                      .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                  .build());
      long endTime = System.currentTimeMillis();
      searchTime = (endTime - startTime) / 1000.00;
      log.info(search.getStatus().toString());
      log.info("search cost:" + searchTime);
    }
  }

  @Test(description = "queryRateTest")
  public void queryRateTest() {
    List<Long> book_id_array = new ArrayList<>();
    for (long i = 0; i < 10; ++i) {
      book_id_array.add(i);
    }
    String SEARCH_PARAM = "book_id in " + book_id_array;
    List<String> outFields = Arrays.asList("book_id");
    double queryTime = 0.00;
    for (int i = 0; i < 5000; i++) {
      long startTime = System.currentTimeMillis();

      milvusClient.loadCollection(
          LoadCollectionParam.newBuilder().withCollectionName("book128").build());
      QueryParam queryParam =
          QueryParam.newBuilder()
              .withCollectionName("book128")
              .withOutFields(outFields)
              .withExpr(SEARCH_PARAM)
              .build();
      R<QueryResults> queryResultsR = milvusClient.query(queryParam);
      log.info(queryResultsR.getStatus().toString());
      long endTime = System.currentTimeMillis();
      queryTime = (endTime - startTime) / 1000.00;
      log.info("query cost:" + queryTime);
    }
  }

  @Test(description = "queryRateTest2")
  public void queryRateTest2() {
    List<Long> book_id_array = new ArrayList<>();
    for (long i = 0; i < 10; ++i) {
      book_id_array.add(i);
    }
    String SEARCH_PARAM = "book_id in " + book_id_array;
    List<String> outFields = Arrays.asList("book_id");
    int threads=20;
    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    milvusClient.loadCollection(
            LoadCollectionParam.newBuilder().withCollectionName("book128").build());
    for (int e = 0; e < threads; e++) {
      Runnable runnable = new Runnable() {
        @Override
        public void run() {
          double queryTime = 0.00;
          for (int i = 0; i < 10000; i++) {
            long startTime = System.currentTimeMillis();


            QueryParam queryParam =
                    QueryParam.newBuilder()
                            .withCollectionName("book128")
                            .withOutFields(outFields)
                            .withExpr(SEARCH_PARAM)
                            .build();
            R<QueryResults> queryResultsR = milvusClient.query(queryParam);
            log.info(queryResultsR.getStatus().toString());
            long endTime = System.currentTimeMillis();
            queryTime = (endTime - startTime) / 1000.00;
            log.info("query cost:" + queryTime);
          }
        }

      };
      executorService.execute(runnable);
    }
    executorService.shutdown();
    try {
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.warn(e.getMessage());
    }
  }
}

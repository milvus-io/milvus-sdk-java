package com.zilliz.milvustest.search;

import com.google.common.util.concurrent.ListenableFuture;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.SearchResultsWrapper;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.zilliz.milvustest.util.MathUtil.combine;

@Epic("Search")
@Feature("SearchAsync")
public class SearchAsyncTest extends BaseTest {
  public String newBookName;
  public String newBookNameBin;

  @BeforeClass(description = "load collection first",alwaysRun = true)
  public void loadCollection() {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .build());
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .build());
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .build());
  }

  @AfterClass(description = "release collection after test",alwaysRun = true)
  public void releaseCollection() {
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .build());
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .build());
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .build());
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .build());
  }

  @DataProvider(name = "providerPartition")
  public Object[][] providerPartition() {
    return new Object[][] {{Boolean.FALSE}, {Boolean.TRUE}};
  }

  @DataProvider(name = "providerConsistency")
  public Object[][] providerConsistency() {
    return new Object[][] {
      {ConsistencyLevelEnum.STRONG},
      {ConsistencyLevelEnum.BOUNDED},
      {ConsistencyLevelEnum.EVENTUALLY}
    };
  }

  @DataProvider(name = "IndexTypes")
  public Object[][] provideIndexType() {
    return new Object[][] {
      {IndexType.IVF_FLAT},
      {IndexType.IVF_SQ8},
      {IndexType.IVF_PQ},
      {IndexType.HNSW},
      {IndexType.SCANN},
      {IndexType.GPU_IVF_FLAT},
      {IndexType.GPU_IVF_PQ}
    };
  }

  @DataProvider(name = "MetricType")
  public Object[][] providerMetricType() {
    return new Object[][] {{MetricType.L2}, {MetricType.IP}};
  }

  @DataProvider(name = "FloatIndex")
  public Object[][] providerIndexForFloatCollection() {
    return combine(provideIndexType(), providerMetricType());
  }

  @DataProvider(name = "BinaryIndexTypes")
  public Object[][] provideBinaryIndexType() {
    return new Object[][] {{IndexType.BIN_IVF_FLAT}, {IndexType.BIN_FLAT}};
  }

  @DataProvider(name = "BinaryMetricType")
  public Object[][] providerBinaryMetricType() {
    return new Object[][] {
      {MetricType.HAMMING},
      {MetricType.JACCARD}
    };
  }

  @DataProvider(name = "BinaryIndex")
  public Object[][] providerIndexForBinaryCollection() {
    return combine(provideBinaryIndexType(), providerBinaryMetricType());
  }

  @DataProvider(name = "provideIntExpressions")
  public Object[][] provideIntExpression() {
    return new Object[][] {
      {"book_id > 10"},
      {"book_id >= 10"},
      {"book_id < 10"},
      {"book_id <= 10"},
      {"book_id == 10"},
      {"book_id !=10"},
      {"book_id in [10,20,30,40]"},
      {"book_id not in [10]"},
      {"10 < book_id < 50 "},
      {"50 > book_id > 10 "},
      {"10 <= book_id <=50 "},
      {"10 <= book_id <50 "},
      {"10 < book_id <=50 "},
      {"book_id >10 and word_count > 10010 "},
      {"book_id >10 and word_count >= 10110 "},
      {"book_id in [10,20,30,40] and word_count >= 10010 "},
      {"book_id not in [10,20,30,40] and word_count >= 10010 "},
      {"book_id in [10,20,30,40] and word_count in [10010,10020,10030,10040] "},
      {"book_id not in [10,20,30,40] and word_count not in [10010,10020,10030,10040] "}
    };
  }

  @DataProvider(name = "provideStringExpressions")
  public Object[][] provideStringExpression() {
    return new Object[][] {
      {" book_name > \"10\" "},
      {" book_name > \"a\" "},
      {" book_name >= \"a\" "},
      {" book_name not in [\"a\"] "},
      {" book_name > book_content "},
      {" book_name >= book_content "},
      {" book_name < book_content "},
      {" book_name <= book_content "},
      {" \"10\" < book_name  <= \"a\" "},
      {" \"a\" <= book_name < \"a99\" "},
      {" \"asa\" < book_name <= \"zaa\" "},
      {" \"a\" <= book_name  and book_name >= \"99\" "},
      {" book_name like \"国%\" "},
      {" book_name like \"国%\" and book_name >\"abc\" "},
      {" book_name like \"国%\" and book_content like\"1%\" "},
      {" book_name like \"国%\" and book_content > \"1\" "}
    };
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description =
          "Conducts ANN async search on a vector field. Use expression to do filtering before search.",
      dataProvider = "providerPartition",groups = {"Smoke"})
  public void intPKAndFloatVectorSearchAsync(Boolean usePart) {
    final Integer SEARCH_K = 2; // TopK
    final String SEARCH_PARAM = "{\"nprobe\":100}";
    List<String> search_output_fields = Arrays.asList("book_id", "word_count");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Conduct a hybrid search async", dataProvider = "providerPartition")
  public void intPKAndFloatVectorHybridSearchAsync(Boolean usePart) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id > 1000 ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Conduct a search with  binary vector async",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearchAsync(Boolean usePart) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultBinaryPartition) : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Conduct a hybrid  binary vector search async",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorHybridSearchAsync(Boolean usePart) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultBinaryPartition) : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id > 1000 ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Conduct float vector Async search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndFloatVectorSearchAsync(Boolean usePart) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Conduct float vector async search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndFloatVectorHybridSearchAsync(Boolean usePart) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_name like \"a%\" ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Conduct binary vector async search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndBinaryVectorSearchAsync(Boolean usePart) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionNames(
                usePart
                    ? Arrays.asList(CommonData.defaultStringPKBinaryPartition)
                    : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Conduct binary vector async search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndBinaryVectorHybridSearchAsync(Boolean usePart) {
    Integer SEARCH_K = 20; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionNames(
                usePart
                    ? Arrays.asList(CommonData.defaultStringPKBinaryPartition)
                    : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_name like \"国%\" ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(
          searchResultsWrapper.getFieldData("book_name", 0).size(), SEARCH_K.intValue());
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Async search in nonexistent  partition")
  public void searchAsyncInNonexistentPartition() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(Arrays.asList("nonexistent"))
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 1);
      Assert.assertEquals(
          rListenableFuture.get().getException().getMessage(),
          "partition name nonexistent not found");
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "search async float vector  with error vectors value)")
  public void searchAsyncWithErrorVectors() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(2)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 1);
      Assert.assertTrue(
          rListenableFuture.get().getException().getMessage().contains("fail to search"));
      System.out.println(rListenableFuture.get().getException().getMessage());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "search async binary vector with error MetricType",
      expectedExceptions = ParamException.class)
  public void binarySearchAsyncWithErrorMetricType() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withMetricType(MetricType.IP)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id > 1000 ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 1);
      Assert.assertTrue(
          rListenableFuture
              .get()
              .getException()
              .getMessage()
              .contains("binary search not support metric type: METRIC_INNER_PRODUCT"));
      System.out.println(rListenableFuture.get().getException().getMessage());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "search async  with error MetricType ",
      expectedExceptions = ParamException.class)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/313")
  public void searchAsyncWithErrorMetricType() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id > 1000 ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 1);
      Assert.assertTrue(
          rListenableFuture
              .get()
              .getException()
              .getMessage()
              .contains("Target vector is float but metric type is incorrect"));
      System.out.println(rListenableFuture.get().getException().getMessage());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "search async with empty vector", expectedExceptions = ParamException.class)
  public void searchAsyncWithEmptyVector() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = new ArrayList<>();
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id > 1000 ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 1);
      Assert.assertTrue(
          rListenableFuture
              .get()
              .getException()
              .getMessage()
              .contains("Target vectors can not be empty"));
      System.out.println(rListenableFuture.get().getException().getMessage());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.MINOR)
  @Test(
      description = "binary search async with empty vector",
      expectedExceptions = ParamException.class)
  public void binarySearchAsyncWithEmptyVector() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<ByteBuffer> search_vectors = new ArrayList<>();
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id > 1000 ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 1);
      Assert.assertTrue(
          rListenableFuture
              .get()
              .getException()
              .getMessage()
              .contains("Target vectors can not be empty"));
      System.out.println(rListenableFuture.get().getException().getMessage());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and float vector search async after insert the entity",
      dataProvider = "providerPartition")
  public void intPKAndFloatVectorSearchAsyncAfterInsertNewEntity(Boolean usePart)
      throws InterruptedException {
    // insert entity first
    List<Long> book_id_array =
        new ArrayList<Long>() {
          {
            add(9999L);
          }
        };
    List<Long> word_count_array =
        new ArrayList<Long>() {
          {
            add(19999L);
          }
        };
    List<Float> fs = Arrays.asList(MathUtil.generateFloat(128));
    List<List<Float>> book_intro_array =
        new ArrayList<List<Float>>() {
          {
            add(fs);
          }
        };
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id",  book_id_array));
    fields.add(new InsertParam.Field("word_count",  word_count_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultVectorField,  book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(usePart ? CommonData.defaultPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id", "word_count");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr("book_id == 9999")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).get(0), 9999L);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and binary vector search async after insert the entity",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearchAsyncAfterInsertNewEntity(Boolean usePart)
      throws InterruptedException {
    // insert entity first
    List<Long> book_id_array =
        new ArrayList<Long>() {
          {
            add(9999L);
          }
        };
    List<Long> word_count_array =
        new ArrayList<Long>() {
          {
            add(19999L);
          }
        };
    List<ByteBuffer> book_intro_array = CommonFunction.generateBinaryVectors(1, 128);

    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id",  book_id_array));
    fields.add(new InsertParam.Field("word_count", word_count_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultBinaryVectorField,  book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionName(usePart ? CommonData.defaultBinaryPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id", "word_count");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultBinaryPartition) : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).get(0), 9999L);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search async after insert the entity",
      dataProvider = "providerPartition")
  public void stringPKAndFloatVectorSearchAsyncAfterInsertNewEntity(Boolean usePart)
      throws InterruptedException {
    // insert entity first
    newBookName = MathUtil.genRandomStringAndChinese(10);
    String newBookContent = MathUtil.genRandomStringAndChinese(20);
    System.out.println("newBookContent:" + newBookContent);
    List<String> book_name_array =
        new ArrayList<String>() {
          {
            add(newBookName);
          }
        };
    List<String> book_content_array =
        new ArrayList<String>() {
          {
            add(newBookContent);
          }
        };
    List<Float> fs = Arrays.asList(MathUtil.generateFloat(128));
    List<List<Float>> book_intro_array =
        new ArrayList<List<Float>>() {
          {
            add(fs);
          }
        };
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name",  book_name_array));
    fields.add(new InsertParam.Field("book_content",  book_content_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultVectorField,  book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name", "book_content");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr("book_name == \"" + newBookName + "\"")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(
          searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookName);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and binary vector search async after insert the entity",
      dataProvider = "providerPartition")
  public void stringPKAndBinaryVectorSearchAsyncAfterInsertNewEntity(Boolean usePart)
      throws InterruptedException {
    // insert entity first
    newBookNameBin = MathUtil.genRandomStringAndChinese(10);
    String newBookContent = MathUtil.genRandomStringAndChinese(20);
    List<String> book_name_array =
        new ArrayList<String>() {
          {
            add(newBookNameBin);
          }
        };
    List<String> book_content_array =
        new ArrayList<String>() {
          {
            add(newBookContent);
          }
        };
    List<ByteBuffer> book_intro_array = CommonFunction.generateBinaryVectors(1, 128);
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name",  book_name_array));
    fields.add(new InsertParam.Field("book_content",  book_content_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultBinaryVectorField,  book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKBinaryPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name", "book_content");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionNames(
                usePart
                    ? Arrays.asList(CommonData.defaultStringPKBinaryPartition)
                    : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(
          searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookNameBin);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and float vector search async after update the entity",
      dataProvider = "providerPartition")
  public void intPKAndFloatVectorSearchAsyncAfterUpdateEntity(Boolean usePart)
      throws InterruptedException {
    Random random = new Random();
    int id = random.nextInt(2000);
    // update entity first
    List<Long> book_id_array =
        new ArrayList<Long>() {
          {
            add((long) id);
          }
        };
    List<Long> word_count_array =
        new ArrayList<Long>() {
          {
            add(19999L);
          }
        };
    List<Float> fs = Arrays.asList(MathUtil.generateFloat(128));
    List<List<Float>> book_intro_array =
        new ArrayList<List<Float>>() {
          {
            add(fs);
          }
        };
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id",  book_id_array));
    fields.add(new InsertParam.Field("word_count",  word_count_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultVectorField,  book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionName(usePart ? CommonData.defaultPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id", "word_count");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr("book_id =="+id)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and binary vector search async after update the entity",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearchAsyncAfterUpdateEntity(Boolean usePart)
      throws InterruptedException {
    Random random = new Random();
    int id = random.nextInt(2000);
    // update entity first
    List<Long> book_id_array =
        new ArrayList<Long>() {
          {
            add((long) id);
          }
        };
    List<Long> word_count_array =
        new ArrayList<Long>() {
          {
            add(19999L);
          }
        };
    List<ByteBuffer> book_intro_array = CommonFunction.generateBinaryVectors(1, 128);

    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_id",  book_id_array));
    fields.add(new InsertParam.Field("word_count",  word_count_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultBinaryVectorField,  book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionName(usePart ? CommonData.defaultBinaryPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id", "word_count");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultBinaryPartition) : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search async after update the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = "stringPKAndFloatVectorSearchAsyncAfterInsertNewEntity")
  public void stringPKAndFloatVectorSearchAsyncAfterUpdateNewEntity(Boolean usePart)
      throws InterruptedException {
    // delete entity first
    milvusClient.delete(
        DeleteParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKPartition : "")
            .withExpr("book_name in [\"" + newBookName + "\"]")
            .build());
    Thread.sleep(4000);
    // update entity first
    String newBookContent = MathUtil.genRandomStringAndChinese(20);
    System.out.println("newBookContent:" + newBookContent);
    List<String> book_name_array =
        new ArrayList<String>() {
          {
            add(newBookName);
          }
        };
    List<String> book_content_array =
        new ArrayList<String>() {
          {
            add(newBookContent);
          }
        };
    List<Float> fs = Arrays.asList(MathUtil.generateFloat(128));
    List<List<Float>> book_intro_array =
        new ArrayList<List<Float>>() {
          {
            add(fs);
          }
        };
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name", book_name_array));
    fields.add(new InsertParam.Field("book_content",  book_content_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultVectorField, book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name", "book_content");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
            .withExpr("book_name == \"" + newBookName + "\"")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(
          searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookName);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and binary vector search async after update the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = "stringPKAndBinaryVectorSearchAsyncAfterInsertNewEntity")
  public void stringPKAndBinaryVectorSearchAsyncAfterUpdateNewEntity(Boolean usePart)
      throws InterruptedException {
    // delete entity first
    milvusClient.delete(
        DeleteParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKBinaryPartition : "")
            .withExpr("book_name in [\"" + newBookNameBin + "\"]")
            .build());
    Thread.sleep(2000);
    // insert entity first
    String newBookContent = MathUtil.genRandomStringAndChinese(20);
    List<String> book_name_array =
        new ArrayList<String>() {
          {
            add(newBookNameBin);
          }
        };
    List<String> book_content_array =
        new ArrayList<String>() {
          {
            add(newBookContent);
          }
        };
    List<ByteBuffer> book_intro_array = CommonFunction.generateBinaryVectors(1, 128);
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name",  book_name_array));
    fields.add(new InsertParam.Field("book_content",  book_content_array));
    fields.add(
        new InsertParam.Field(
            CommonData.defaultBinaryVectorField,  book_intro_array));
    milvusClient.insert(
        InsertParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKBinaryPartition : "")
            .withFields(fields)
            .build());
    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name", "book_content");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionNames(
                usePart
                    ? Arrays.asList(CommonData.defaultStringPKBinaryPartition)
                    : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
            .withExpr("book_name == \"" + newBookNameBin + "\"")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(
          searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookNameBin);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and float vector search async after delete data",
      dataProvider = "providerPartition")
  public void intPKAndFloatVectorSearchAsyncAfterDelete(Boolean usePart)
      throws InterruptedException {
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(usePart ? CommonData.defaultPartition : "")
                .withExpr("book_id in [1,2,3]")
                .build());
    Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 3L);
    Thread.sleep(2000);
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id in [1,2,3] ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResults searchResults = rListenableFuture.get().getData();
      Assert.assertEquals(searchResults.getResults().getFieldsDataCount(), 0);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and binary vector search async after delete data",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearchAsyncAfterDelete(Boolean usePart)
      throws InterruptedException {
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(CommonData.defaultBinaryCollection)
                .withPartitionName(usePart ? CommonData.defaultBinaryPartition : "")
                .withExpr("book_id in [1,2,3]")
                .build());
    Assert.assertEquals(mutationResultR.getData().getDeleteCnt(), 3L);
    Thread.sleep(2000);
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultBinaryPartition) : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(" book_id in [1,2,3] ")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResults searchResults = rListenableFuture.get().getData();
      Assert.assertEquals(searchResults.getResults().getFieldsDataCount(), 0);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search after delete the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = {
        "stringPKAndFloatVectorSearchAsyncAfterInsertNewEntity",
        "stringPKAndFloatVectorSearchAsyncAfterUpdateNewEntity"
      })
  public void stringPKAndFloatVectorSearchAsyncAfterDelete(Boolean usePart)
      throws InterruptedException {
    // delete entity first
    milvusClient.delete(
        DeleteParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKPartition : "")
            .withExpr("book_name in [\"" + newBookName + "\"]")
            .build());
    Thread.sleep(2000);

    List<Float> fs = Arrays.asList(MathUtil.generateFloat(128));
    List<List<Float>> book_intro_array =
        new ArrayList<List<Float>>() {
          {
            add(fs);
          }
        };

    Thread.sleep(2000);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name", "book_content");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr("book_name in [\"" + newBookName + "\"]")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResults searchResults = rListenableFuture.get().getData();
      Assert.assertEquals(searchResults.getStatus().getReason(), "search result is empty");
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search async after delete the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = {
        "stringPKAndBinaryVectorSearchAsyncAfterInsertNewEntity",
        "stringPKAndBinaryVectorSearchAsyncAfterUpdateNewEntity"
      })
  public void stringPKAndBinaryVectorSearchAsyncAfterDelete(Boolean usePart)
      throws InterruptedException {
    // delete entity first
    milvusClient.delete(
        DeleteParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKBinaryPartition : "")
            .withExpr("book_name in [\"" + newBookNameBin + "\"]")
            .build());
    Thread.sleep(2000);
    List<ByteBuffer> book_intro_array = CommonFunction.generateBinaryVectors(1, 128);
    // search
    Integer SEARCH_K = 1; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name", "book_content");

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionNames(
                usePart
                    ? Arrays.asList(CommonData.defaultStringPKBinaryPartition)
                    : Arrays.asList())
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(book_intro_array)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr("book_name in [\"" + newBookNameBin + "\"]")
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResults searchResults = rListenableFuture.get().getData();
      Assert.assertEquals(searchResults.getStatus().getReason(), "search result is empty");
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "int PK and float vector search async by alias")
  public void intPKAndFloatVectorSearchAsyncByAlias() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultAlias)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "int PK and float vector search async by alias")
  public void intPKAndFloatVectorSearchAsyncByNonexistentAlias() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName("NonexistentAlias")
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 1);
      Assert.assertEquals(
          rListenableFuture.get().getException().getMessage(),
          "DescribeCollection failed: can't find collection: NonexistentAlias");
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "int pk and binary vector search async by alias")
  public void intPKAndBinaryVectorSearchAsyncByAlias() {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryAlias)
            .withMetricType(MetricType.JACCARD)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Int pk search async with each consistency level",
      dataProvider = "providerConsistency")
  public void intPKSearchAsyncWithConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withConsistencyLevel(consistencyLevel)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "String pk search async with each consistency level",
      dataProvider = "providerConsistency")
  public void stringPKSearchAsyncWithConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withConsistencyLevel(consistencyLevel)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Int PK and float vector search async with each index",
      dataProvider = "FloatIndex")
  public void intPKAndFloatVectorSearchAsyncWithEachIndex(
      IndexType indexType, MetricType metricType) {
    String newCollection = CommonFunction.createNewCollection();
    // create index
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(newCollection)
                .withFieldName(CommonData.defaultVectorField)
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(metricType)
                .withIndexType(indexType)
                .withExtraParam(CommonFunction.provideExtraParam(indexType))
                .withSyncMode(Boolean.FALSE)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    // Insert test data
    List<InsertParam.Field> fields = CommonFunction.generateData(1000);
    milvusClient.insert(
        InsertParam.newBuilder().withCollectionName(newCollection).withFields(fields).build());
    // load
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(newCollection)
            .withSyncLoad(true)
            .withSyncLoadWaitingInterval(500L)
            .withSyncLoadWaitingTimeout(30L)
            .build());
    // search
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(newCollection)
            .withMetricType(metricType)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
    // drop collection
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "String PK and Binary vector search async with each index",
      dataProvider = "BinaryIndex")
  public void stringPKAndBinaryVectorSearchAsyncWithEachIndex(
      IndexType indexType, MetricType metricType) {
    String stringPKAndBinaryCollection = CommonFunction.createStringPKAndBinaryCollection();
    // create index
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(stringPKAndBinaryCollection)
                .withFieldName(CommonData.defaultBinaryVectorField)
                .withIndexName(CommonData.defaultBinaryIndex)
                .withMetricType(metricType)
                .withIndexType(indexType)
                .withExtraParam(CommonData.defaultExtraParam)
                .withSyncMode(Boolean.FALSE)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    // Insert test data
    List<InsertParam.Field> fields = CommonFunction.generateStringPKBinaryData(2000);
    milvusClient.insert(
        InsertParam.newBuilder()
            .withFields(fields)
            .withCollectionName(stringPKAndBinaryCollection)
            .build());
    // load
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(stringPKAndBinaryCollection)
            .withSyncLoad(true)
            .withSyncLoadWaitingInterval(500L)
            .withSyncLoadWaitingTimeout(30L)
            .build());
    // search
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<ByteBuffer> search_vectors = CommonFunction.generateBinaryVectors(1, 128);

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(stringPKAndBinaryCollection)
            .withMetricType(metricType)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultBinaryVectorField)
            .withParams(SEARCH_PARAM)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
    // drop collection
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(stringPKAndBinaryCollection).build());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Int PK search async with each expression",
      dataProvider = "provideIntExpressions")
  public void intPKSearchAsyncWithEachExpressions(String express) {
    Integer SEARCH_K = 4; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(express)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertTrue(searchResultsWrapper.getFieldData("book_id", 0).size() >= 1);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "String PK search async with each expressions",
      dataProvider = "provideStringExpressions")
  public void stringPKSearchAsyncWithEachExpressions(String expression) {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
        SearchParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withMetricType(MetricType.L2)
            .withOutFields(search_output_fields)
            .withTopK(SEARCH_K)
            .withVectors(search_vectors)
            .withVectorFieldName(CommonData.defaultVectorField)
            .withParams(SEARCH_PARAM)
            .withExpr(expression)
            .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    try {
      Assert.assertEquals(rListenableFuture.get().getStatus().intValue(), 0);
      SearchResultsWrapper searchResultsWrapper =
          new SearchResultsWrapper(rListenableFuture.get().getData().getResults());
      Assert.assertTrue(searchResultsWrapper.getFieldData("book_name", 0).size() >= 1);
      System.out.println(rListenableFuture.get().getData());
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Search without load")
  public void searchAsyncWithoutLoad() throws ExecutionException, InterruptedException {
    milvusClient.releaseCollection(ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .build());
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName(CommonData.defaultVectorField)
                    .withParams(SEARCH_PARAM)
                    .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    R<SearchResults> searchResultsR = rListenableFuture.get();
    Assert.assertEquals(searchResultsR.getStatus().intValue(),1);
    Assert.assertTrue(searchResultsR.getException().getMessage().contains("checkIfLoaded failed when search"));
    milvusClient.loadCollection(LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "string PK Search with error expressions")
  public void stringPKSearchAsyncWithErrorExpressions() throws ExecutionException, InterruptedException {
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_name");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));

    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(CommonData.defaultStringPKCollection)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName(CommonData.defaultVectorField)
                    .withParams(SEARCH_PARAM)
                    .withExpr( "  book_name = a")
                    .build();
    ListenableFuture<R<SearchResults>> rListenableFuture = milvusClient.searchAsync(searchParam);
    R<SearchResults> searchResultsR = rListenableFuture.get();
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(searchResultsR.getException().getMessage().contains("cannot parse expression"));

  }
}

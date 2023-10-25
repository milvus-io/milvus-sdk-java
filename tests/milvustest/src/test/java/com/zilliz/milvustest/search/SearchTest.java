package com.zilliz.milvustest.search;

import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
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
import io.milvus.response.FieldDataWrapper;
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

import static com.zilliz.milvustest.util.MathUtil.combine;

@Epic("Search")
@Feature("Search")
public class SearchTest extends BaseTest {
  public String newBookName;
  public String newBookNameBin;
  public String collectionWithJsonField;
  public String collectionWithDynamicField;

  public String collectionWithArrayField;

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
    collectionWithJsonField= CommonFunction.createNewCollectionWithJSONField();
    collectionWithDynamicField= CommonFunction.createNewCollectionWithDynamicField();
    collectionWithArrayField= CommonFunction.createNewCollectionWithArrayField();
  }

  @DataProvider(name="dynamicExpressions")
  public Object[][] provideDynamicExpression(){
    return  new Object[][]{
            {"json_field[\"int32\"] in [2,4,6,8]"},
            {"book_id in [10,20,30,40]"},
            {"extra_field2 in [1,2,3,4]"},
            {"\"String0\"<=extra_field<=\"String3\""}
    };
  }
  @DataProvider(name="jsonExpressions")
  public Object[][] provideJsonExpression(){
    return  new Object[][]{
            {"int64_field in [10,20,30,40]"},
            {"json_field[\"int64_field\"] in [10,20,30,40]"},
            {"json_field[\"inner_json\"][\"int32\"] in [1,2,3,4]"},
            {"\"Str0\"<=json_field[\"inner_json\"][\"varchar\"]<=\"Str3\""},
            {"json_field[\"inner_json\"][\"int64\"] in [10,20,30,40]"}
    };
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
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithJsonField).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithDynamicField).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithArrayField).build());

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

  @DataProvider(name="provideIntExpressions")
  public Object[][] provideIntExpression(){
    return new Object[][]{
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
  @DataProvider(name="provideStringExpressions")
  public Object[][] provideStringExpression(){
    return new Object[][]{
            {" book_name > \"10\" "},
            {" book_name > \"a\" "},
            {" book_name >= \"a\" "},
            {" book_name not in [\"a\"] "},
            {" book_name > book_content "},
            {" book_name >= book_content "},
            {" book_name < book_content "},
            {" book_name <= book_content "},
            {" \"10\" < book_name  <= \"a\" "},
            {" \"a\" <= book_name < \"zAs\" "},
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
          "Conducts ANN search on a vector field. Use expression to do filtering before search.",
      dataProvider = "providerPartition",groups = {"Smoke"})
  public void intPKAndFloatVectorSearch(Boolean usePart) {
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
                .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
            .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Conduct a hybrid search", dataProvider = "providerPartition")
  public void intPKAndFloatVectorHybridSearch(Boolean usePart) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertTrue(searchResultsR.getData().getResults().getIds().getIntId().getData(0) > 1000);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Conduct a search with  binary vector", dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearch(Boolean usePart) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    System.out.println(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsR.getData().getResults().getTopK(), 2);
    Assert.assertEquals(
        searchResultsR.getData().getResults().getIds().getIntId().getDataCount(), 2);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Conduct a hybrid  binary vector search", dataProvider = "providerPartition")
  public void intPKAndBinaryVectorHybridSearch(Boolean usePart) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(searchResultsR.getData().getResults().getIds().getIntId().getData(0) > 1000);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Conduct float vector search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndFloatVectorSearch(Boolean usePart) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Conduct float vector search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndFloatVectorHybridSearch(Boolean usePart) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(
      description = "Conduct binary vector search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndBinaryVectorSearch(Boolean usePart) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "Conduct binary vector search with String PK",
      dataProvider = "providerPartition")
  public void stringPKAndBinaryVectorHybridSearch(Boolean usePart) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(
        searchResultsWrapper.getFieldData("book_name", 0).size(), SEARCH_K.intValue());
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "search in nonexistent  partition")
  public void searchInNonexistentPartition() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertEquals(
        searchResultsR.getException().getMessage(), "partition name nonexistent not found");
    System.out.println(searchResultsR);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "search float vector  with error vectors value)")
  public void searchWithErrorVectors() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(searchResultsR.getException().getMessage().contains("fail to search"));
    System.out.println(searchResultsR.getException().getMessage());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "search binary vector with error MetricType",
      expectedExceptions = ParamException.class)
  public void binarySearchWithErrorMetricType() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(
        searchResultsR
            .getException()
            .getMessage()
            .contains("binary search not support metric type: METRIC_INNER_PRODUCT"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "search with error MetricType", expectedExceptions = ParamException.class)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/313")
  public void SearchWithErrorMetricType() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(
        searchResultsR
            .getException()
            .getMessage()
            .contains("binary search not support metric type: METRIC_INNER_JACCARD"));
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "search with empty vector", expectedExceptions = ParamException.class)
  public void searchWithEmptyVector() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(
        searchResultsR.getException().getMessage().contains("Target vectors can not be empty"));
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "binary search with empty vector", expectedExceptions = ParamException.class)
  public void binarySearchWithEmptyVector() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(
        searchResultsR.getException().getMessage().contains("Target vectors can not be empty"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and float vector search after insert the entity",
      dataProvider = "providerPartition")
  public void intPKAndFloatVectorSearchAfterInsertNewEntity(Boolean usePart)
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
    fields.add(new InsertParam.Field("word_count", word_count_array));
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).get(0), 9999L);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and binary vector search after insert the entity",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearchAfterInsertNewEntity(Boolean usePart)
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
            .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
            .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).get(0), 9999L);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search after insert the entity",
      dataProvider = "providerPartition")
  public void stringPKAndFloatVectorSearchAfterInsertNewEntity(Boolean usePart)
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(
        searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookName);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and binary vector search after insert the entity",
      dataProvider = "providerPartition")
  public void stringPKAndBinaryVectorSearchAfterInsertNewEntity(Boolean usePart)
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
                .withExpr("book_name == \"" + newBookNameBin + "\"")
            .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(
        searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookNameBin);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and float vector search after update the entity",
      dataProvider = "providerPartition")
  public void intPKAndFloatVectorSearchAfterUpdateEntity(Boolean usePart)
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
            .withExpr("book_id == "+id)
            .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and binary vector search after update the entity",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearchAfterUpdateEntity(Boolean usePart)
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("word_count", 0).get(0), 19999L);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search after update the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = "stringPKAndFloatVectorSearchAfterInsertNewEntity")
  public void stringPKAndFloatVectorSearchAfterUpdateNewEntity(Boolean usePart)
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(
        searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookName);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search after update the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = "stringPKAndBinaryVectorSearchAfterInsertNewEntity")
  public void stringPKAndBinaryVectorSearchAfterUpdateNewEntity(Boolean usePart)
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
    fields.add(new InsertParam.Field("book_content", book_content_array));
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(
        searchResultsWrapper.getFieldData("book_content", 0).get(0), newBookContent);
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).get(0), newBookNameBin);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and float vector search after delete data",
      dataProvider = "providerPartition")
  public void intPKAndFloatVectorSearchAfterDelete(Boolean usePart) throws InterruptedException {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    System.out.println(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(searchResultsR.getData().getResults().getFieldsDataCount(), 0);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "int PK and binary vector search after delete data",
      dataProvider = "providerPartition")
  public void intPKAndBinaryVectorSearchAfterDelete(Boolean usePart) throws InterruptedException {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    System.out.println(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(searchResultsR.getData().getResults().getFieldsDataCount(), 0);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and float vector search after delete the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = {
        "stringPKAndFloatVectorSearchAfterInsertNewEntity",
        "stringPKAndFloatVectorSearchAfterUpdateNewEntity"
      })
  public void stringPKAndFloatVectorSearchAfterDelete(Boolean usePart) throws InterruptedException {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(searchResultsR.getData().getStatus().getReason(), "search result is empty");
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "string PK and Binary vector search after delete the entity",
      dataProvider = "providerPartition",
      dependsOnMethods = {
        "stringPKAndBinaryVectorSearchAfterInsertNewEntity",
        "stringPKAndBinaryVectorSearchAfterUpdateNewEntity"
      })
  public void stringPKAndBinaryVectorSearchAfterDelete(Boolean usePart)
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(searchResultsR.getData().getStatus().getReason(), "search result is empty");
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "int PK and float vector search by alias")
  public void intPKAndFloatVectorSearchByAlias() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsR.getData().getResults().getTopK(), 2);
    Assert.assertEquals(
        searchResultsR.getData().getResults().getIds().getIntId().getDataCount(), 2);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "int PK and float vector search by alias")
  public void intPKAndFloatVectorSearchByNonexistentAlias() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertEquals(
        searchResultsR.getException().getMessage(),
        "DescribeCollection failed: can't find collection: NonexistentAlias");
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "int pk and binary vector search by alias")
  public void intPKAndBinaryVectorSearchByAlias() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    System.out.println(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsR.getData().getResults().getTopK(), 2);
    Assert.assertEquals(
        searchResultsR.getData().getResults().getIds().getIntId().getDataCount(), 2);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "search with each consistency level", dataProvider = "providerConsistency")
  public void intPKSearchWithConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "String Pk and  float vector search with consistency",
      dataProvider = "providerConsistency")
  public void stringPKSearchWithConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Int PK and float vector search with each index", dataProvider = "FloatIndex")
  public void intPKAndFloatVectorSearchWithEachIndex(IndexType indexType, MetricType metricType) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
    // drop collection
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "String PK and Binary vector search with each index",
      dataProvider = "BinaryIndex")
  public void stringPKAndBinaryVectorSearchWithEachIndex(
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    System.out.println(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
        new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_name", 0).size(), 2);
    // drop collection
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(stringPKAndBinaryCollection).build());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Int PK search with each expression", dataProvider = "provideIntExpressions")
  public void intPKSearchWithEachExpressions(String express) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertTrue(searchResultsWrapper.getFieldData("book_id",0).size() >= 1);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
          description = "string PK Search with each expressions",
          dataProvider = "provideStringExpressions")
  public void stringPKSearchWithEachExpressions(String expression) {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertTrue(searchResultsWrapper.getFieldData("book_name", 0).size()>=1);
    System.out.println(searchResultsR.getData().getResults());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Search without load")
  public void searchWithoutLoad(){
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(),1);
    Assert.assertTrue(searchResultsR.getException().getMessage().contains("not loaded into memory"));
    milvusClient.loadCollection(LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "string PK Search with error expressions")
  public void stringPKSearchWithErrorExpressions() {
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(searchResultsR.getException().getMessage().contains("cannot parse expression"));

  }

  public long data1=0L;
  public long data2=0L;

  @Severity(SeverityLevel.BLOCKER)
  @Test(
          description =
                  "Search with pagination(offset=0)",groups = {"Smoke"})
  public void intPKAndFloatVectorSearchWithPagination() {
    Integer SEARCH_K = 4; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10,\"offset\":0}";
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
                    .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 4);
    data1=searchResultsR.getData().getResults().getIds().getIntId().getData(2);
    data2=searchResultsR.getData().getResults().getIds().getIntId().getData(3);
  }


  @Severity(SeverityLevel.BLOCKER)
  @Test(
          description =
                  "Search with pagination.",
          groups = {"Smoke"},dependsOnMethods = "intPKAndFloatVectorSearchWithPagination")
  public void intPKAndFloatVectorSearchWithPagination2() {
    Integer SEARCH_K = 4; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10,\"offset\":2}";
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
                    .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), 4);
    Assert.assertEquals(searchResultsR.getData().getResults().getIds().getIntId().getData(0),data1);
    Assert.assertEquals(searchResultsR.getData().getResults().getIds().getIntId().getData(1),data2);
    logger.info(searchResultsR.getData().getResults().toString());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Search with DynamicField.", groups = {"Smoke"},dataProvider = "dynamicExpressions")
  public void searchWithDynamicField(String expr){
    List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithDynamicField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(),0);
    CommonFunction.createIndexWithLoad(collectionWithDynamicField,IndexType.HNSW,MetricType.L2,CommonData.defaultVectorField);
    // search
    Integer SEARCH_K = 10; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("extra_field");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(collectionWithDynamicField)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName(CommonData.defaultVectorField)
                    .withParams(SEARCH_PARAM)
                    .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                    .withExpr(expr)
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertTrue(searchResultsWrapper.getFieldData("$meta", 0).size()>=4);
    logger.info(searchResultsR.getData().getResults().toString());
    System.out.println(searchResultsWrapper.getFieldData("$meta", 0).size());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Search with DynamicField use nonexistent field name")
  public void searchWithDynamicFieldUseNonexistentFiledName(){
    List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithDynamicField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(),0);
    CommonFunction.createIndexWithLoad(collectionWithDynamicField,IndexType.HNSW,MetricType.L2,CommonData.defaultVectorField);
    // search
    Integer SEARCH_K = 10; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("extra_field_nonexistent");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(collectionWithDynamicField)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName(CommonData.defaultVectorField)
                    .withParams(SEARCH_PARAM)
                    .withConsistencyLevel(ConsistencyLevelEnum.BOUNDED)
                    .withExpr(" extra_field2 > 100 ")
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData("$meta", 0).size(), 10);
    logger.info(searchResultsR.getData().getResults().toString());

  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Search with JSON field.", groups = {"Smoke"},dataProvider = "jsonExpressions")
  public void searchWithJsonField(String expr) {
    List<JSONObject> jsonObjects = CommonFunction.generateJsonData(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithJsonField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    CommonFunction.createIndexWithLoad(collectionWithJsonField, IndexType.HNSW, MetricType.L2, "float_vector");
    // search
    Integer SEARCH_K = 10; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("json_field");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(collectionWithJsonField)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName("float_vector")
                    .withParams(SEARCH_PARAM)
                    .withExpr(expr)
                    .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertTrue(searchResultsWrapper.getFieldData("json_field", 0).size()>=4);
    // 按照列获取数据
    FieldDataWrapper json_field = searchResultsWrapper.getFieldWrapper("json_field");
    String string_field = json_field.getAsString(0, "string_field");
    Assert.assertTrue(string_field.contains("Str"));
    // 按照行
    JSONObject jsonObject= (JSONObject) searchResultsWrapper.getIDScore(0).get(0).get("json_field");
    String string = jsonObject.getString("string_field");
    Assert.assertTrue(string.contains("Str"));


  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Int PK and float vector search with each index", dataProvider = "FloatIndex")
  public void searchReturnVector(IndexType indexType, MetricType metricType) {
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
    List<String> search_output_fields = Arrays.asList("book_id",CommonData.defaultVectorField);
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
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertEquals(searchResultsWrapper.getFieldData(CommonData.defaultVectorField, 0).size(), 2);
    System.out.println(searchResultsR.getData().getResults());
    // drop collection
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Search with array field",groups = {"Smoke"})
  public void searchWithArrayField(){
    List<JSONObject> jsonObjects = CommonFunction.generateJsonDataWithArrayField(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithArrayField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    CommonFunction.createIndexWithLoad(collectionWithArrayField, IndexType.HNSW, MetricType.L2, "float_vector");
    // search
    Integer SEARCH_K = 10; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("str_array_field","int_array_field","float_array_field");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(collectionWithArrayField)
                    .withMetricType(MetricType.L2)
                    .withOutFields(search_output_fields)
                    .withTopK(SEARCH_K)
                    .withVectors(search_vectors)
                    .withVectorFieldName("float_vector")
                    .withParams(SEARCH_PARAM)
//                    .withExpr(expr)
                    .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                    .build();
    R<SearchResults> searchResultsR = milvusClient.search(searchParam);
    Assert.assertEquals(searchResultsR.getStatus().intValue(), 0);
    SearchResultsWrapper searchResultsWrapper =
            new SearchResultsWrapper(searchResultsR.getData().getResults());
    Assert.assertTrue(searchResultsWrapper.getFieldData("str_array_field", 0).size()>=4);
  }
}

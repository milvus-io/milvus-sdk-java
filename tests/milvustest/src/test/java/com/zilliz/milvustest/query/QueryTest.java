package com.zilliz.milvustest.query;

import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.QueryResults;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.FieldDataWrapper;
import io.milvus.response.QueryResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.zilliz.milvustest.util.MathUtil.combine;

@Epic("Query")
@Feature("Query")
public class QueryTest extends BaseTest {
  public String newBookName;
  public String collectionWithJsonField;
  public String collectionWithDynamicField;

  public String collectionWithArrayField;

  private String collectionWithSparseVector;

  private String collectionWithMultiVector;
  private String collectionWithFloat16Vector;
  private String collectionWithBf16Vector;


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
    collectionWithSparseVector = CommonFunction.createSparseFloatVectorCollection();
    collectionWithFloat16Vector=CommonFunction.createFloat16Collection();
    collectionWithBf16Vector=CommonFunction.createBf16Collection();
    collectionWithMultiVector = CommonFunction.createMultiVectorCollection();
  }

  @DataProvider(name = "providerPartition")
  public Object[][] providerPartition() {
    return new Object[][] {{Boolean.FALSE}, {Boolean.TRUE}};
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
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithSparseVector).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithMultiVector).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithFloat16Vector).build());
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(collectionWithBf16Vector).build());
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
      {" \"a\" <= book_name < \"zAS\" "},
      {" \"asa\" < book_name <= \"zaa\" "},
      {" \"a\" <= book_name  and book_name >= \"99\" "},
      {" book_name like \"国%\" "},
      {" book_name like \"国%\" and book_name >\"abc\" "},
      {" book_name like \"国%\" and book_content like\"1%\" "},
      {" book_name like \"国%\" and book_content > \"1\" "}
    };
  }

  @DataProvider(name="sparseIndex")
  public Object[][] provideSparseIndex(){
    return new IndexType[][]{
            {IndexType.SPARSE_INVERTED_INDEX}
            ,{IndexType.SPARSE_WAND}
    };
  }

  @Test(description = "int PK and float vector query", dataProvider = "providerPartition",groups = {"Smoke"})
  @Severity(SeverityLevel.BLOCKER)
  public void intPKAndFloatVectorQuery(Boolean usePart) {
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count", CommonData.defaultVectorField);
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper(CommonData.defaultVectorField).getDim(), 128);
  }

  @Test(description = "Int PK and  binary vector query ", dataProvider = "providerPartition")
  @Severity(SeverityLevel.BLOCKER)
  public void intPKAndBinaryVectorQuery(Boolean usePart) {
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields =
        Arrays.asList("book_id", "word_count", CommonData.defaultBinaryVectorField);
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultBinaryPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper(CommonData.defaultBinaryVectorField).getDim());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
    Assert.assertEquals(
        wrapperQuery.getFieldWrapper(CommonData.defaultBinaryVectorField).getDim(), 128);
  }

  @Test(description = "String PK and float vector query", dataProvider = "providerPartition")
  @Severity(SeverityLevel.BLOCKER)
  public void stringPKAndFloatVectorQuery(Boolean usePart) {
    String SEARCH_PARAM = "book_content like \"10%\"";
    List<String> outFields =
        Arrays.asList("book_name", "book_content", CommonData.defaultVectorField);
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() > 10);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() > 10);
    Assert.assertEquals(wrapperQuery.getFieldWrapper(CommonData.defaultVectorField).getDim(), 128);
  }

  @Test(description = "String PK and binary vector query", dataProvider = "providerPartition")
  @Severity(SeverityLevel.BLOCKER)
  public void stringPKAndBinaryVectorQuery(Boolean usePart) {
    String SEARCH_PARAM = "book_content like \"10%\"";
    List<String> outFields =
        Arrays.asList("book_name", "book_content", CommonData.defaultBinaryVectorField);
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withPartitionNames(
                usePart
                    ? Arrays.asList(CommonData.defaultStringPKBinaryPartition)
                    : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() > 10);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() > 10);
    Assert.assertEquals(
        wrapperQuery.getFieldWrapper(CommonData.defaultBinaryVectorField).getDim(), 128);
  }

  @Test(
      description = "Int PK and  float vector query after insert new data",
      dataProvider = "providerPartition")
  @Severity(SeverityLevel.NORMAL)
  public void intPKAndFloatVectorAfterInsert(Boolean usePart) throws InterruptedException {
    // insert entity first
    List<Long> book_id_array =
        new ArrayList<Long>() {
          {
            add(8889L);
          }
        };
    List<Long> word_count_array =
        new ArrayList<Long>() {
          {
            add(18888L);
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
    String SEARCH_PARAM = "book_id == 8889";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().get(0), 8889L);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().get(0), 18888L);
  }

  @Test(
      description = "String PK and  float vector query after insert new data",
      dataProvider = "providerPartition")
  @Severity(SeverityLevel.NORMAL)
  public void stringPKAndFloatVectorAfterInsert(Boolean usePart) throws InterruptedException {
    // insert entity first
    newBookName = MathUtil.genRandomStringAndChinese(10);
    String newBookContent = MathUtil.genRandomStringAndChinese(20);
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
    fields.add(new InsertParam.Field("book_content", book_content_array));
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
    String SEARCH_PARAM = "book_name == \"" + newBookName + "\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(
        wrapperQuery.getFieldWrapper("book_name").getFieldData().get(0), newBookName);
    Assert.assertEquals(
        wrapperQuery.getFieldWrapper("book_content").getFieldData().get(0), newBookContent);
  }

  @Test(
      description = "Int PK and  float vector query after update ",
      dataProvider = "providerPartition",
      dependsOnMethods = "intPKAndFloatVectorAfterInsert")
  @Severity(SeverityLevel.NORMAL)
  public void intPKAndFloatVectorAfterUpdate(Boolean usePart) throws InterruptedException {
    // delete first for update
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(usePart ? CommonData.defaultPartition : "")
                .withExpr("book_id in [8889]")
                .build());
    // update entity first
    List<Long> book_id_array =
        new ArrayList<Long>() {
          {
            add(8889L);
          }
        };
    List<Long> word_count_array =
        new ArrayList<Long>() {
          {
            add(28888L);
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
    String SEARCH_PARAM = "book_id == 8889";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().get(0), 8889L);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().get(0), 28888L);
  }

  @Test(
      description = "String PK and  float vector query after update",
      dataProvider = "providerPartition",
      dependsOnMethods = "stringPKAndFloatVectorAfterInsert")
  @Severity(SeverityLevel.NORMAL)
  public void stringPKAndFloatVectorAfterUpdate(Boolean usePart) throws InterruptedException {
    // delete entity first
    milvusClient.delete(
        DeleteParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionName(usePart ? CommonData.defaultStringPKPartition : "")
            .withExpr("book_name in [\"" + newBookName + "\"]")
            .build());
    // update entity first
    String newBookContent = MathUtil.genRandomStringAndChinese(20);
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
    String SEARCH_PARAM = "book_name == \"" + newBookName + "\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(
        wrapperQuery.getFieldWrapper("book_name").getFieldData().get(0), newBookName);
    Assert.assertEquals(
        wrapperQuery.getFieldWrapper("book_content").getFieldData().get(0), newBookContent);
  }

  @Test(
      description = "Int PK and  float vector query after delete data ",
      dataProvider = "providerPartition")
  @Severity(SeverityLevel.NORMAL)
  public void intPKAndFloatVectorAfterDelete(Boolean usePart) throws InterruptedException {
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionName(usePart ? CommonData.defaultPartition : "")
                .withExpr("book_id in [22,24,26,28]")
                .build());
    Thread.sleep(2000);
    String SEARCH_PARAM = "book_id in [22,24,26,28]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    milvusClient.loadCollection(LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection).build());
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(
        queryResultsR.getException().getMessage(), "empty collection or improper expression");
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 26);
  }

  @Test(
      description = "String PK and float vector query after delete data ",
      dataProvider = "providerPartition",
      dependsOnMethods = {"stringPKAndFloatVectorAfterInsert", "stringPKAndFloatVectorAfterUpdate"})
  @Severity(SeverityLevel.NORMAL)
  public void StringPKAndFloatVectorAfterDelete(Boolean usePart) throws InterruptedException {
    R<MutationResult> mutationResultR =
        milvusClient.delete(
            DeleteParam.newBuilder()
                .withCollectionName(CommonData.defaultStringPKCollection)
                .withPartitionName(usePart ? CommonData.defaultStringPKPartition : "")
                .withExpr("book_name in [\"" + newBookName + "\"]")
                .build());
    Thread.sleep(2000);
    String SEARCH_PARAM = "book_name == \"" + newBookName + "\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withPartitionNames(
                usePart ? Arrays.asList(CommonData.defaultStringPKPartition) : Arrays.asList())
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(
        queryResultsR.getException().getMessage(), "empty collection or improper expression");
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 26);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "query without expression", expectedExceptions = ParamException.class)
  public void queryWithoutExpression() {
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultBinaryCollection)
            .withOutFields(outFields)
            .withExpr("")
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    Assert.assertEquals(
        queryResultsR.getException().getMessage(), "Expression cannot be null or empty");
  }

  @Test(description = "query return empty ")
  @Severity(SeverityLevel.MINOR)
  public void queryReturnEmpty() {
    String SEARCH_PARAM = "book_id in [-1,-2,-3,-4]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 26);
    Assert.assertEquals(
        queryResultsR.getException().getMessage(), "empty collection or improper expression");
  }

  @Test(description = "query float vector with chinese ")
  @Severity(SeverityLevel.NORMAL)
  public void queryByChineseExpress() {
    String SEARCH_PARAM = "book_name like \"国%\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() > 1);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() > 1);
  }

  @Test(description = "query String PK With Complex Express")
  @Severity(SeverityLevel.NORMAL)
  public void queryStringPKWithComplexExpress() {
    String SEARCH_PARAM = "book_name like \"国%\" and book_content like \"1%\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() > 1);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() > 1);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Int pk query  with alias")
  public void intPKQueryWithAlias() {
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultAlias)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "Int PK query  with nonexistent alias")
  public void intPKQueryWithNonexistentAlias() {
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName("NonexistentAlias")
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(
        queryResultsR.getException().getMessage(),
        "DescribeCollection failed: can't find collection: NonexistentAlias");
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
  }

  @Test(description = "String PK and float vector query with alias")
  @Severity(SeverityLevel.CRITICAL)
  public void stringPKQueryWithAlias() {
    String SEARCH_PARAM = "book_content like \"10%\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKAlias)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() > 10);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() > 10);
  }

  @Test(description = "Int PK and float Vector query using each index", dataProvider = "FloatIndex")
  @Severity(SeverityLevel.CRITICAL)
  public void intPKAndFloatVectorQueryUsingEachIndex(IndexType indexType, MetricType metricType) {
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
    // query
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(newCollection)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
    // drop collection
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Test(
      description = "String PK and binary Vector query using each index",
      dataProvider = "BinaryIndex")
  @Severity(SeverityLevel.CRITICAL)
  public void stringPKAndBinaryVectorQueryUsingEachIndex(
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
    // query
    String SEARCH_PARAM = "book_content like \"10%\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(stringPKAndBinaryCollection)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);

    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() > 10);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() > 10);
    // drop collection
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(stringPKAndBinaryCollection).build());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Int PK query with each expression", dataProvider = "provideIntExpressions")
  public void intPKQueryWithEachExpressions(String express) {
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withOutFields(outFields)
            .withExpr(express)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);

    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_id").getFieldData().size() >= 1);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("word_count").getFieldData().size() >= 1);
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "String PK query with each expression",
      dataProvider = "provideStringExpressions")
  public void stringPKQueryWithEachExpressions(String express) {
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKBinaryCollection)
            .withOutFields(outFields)
            .withExpr(express)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);

    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() >= 1);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() >= 1);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
      description = "String PK and  float vector query with consistency",
      dataProvider = "providerConsistency")
  public void stringPKSearchWithConsistencyLevel(ConsistencyLevelEnum consistencyLevel) {
    String SEARCH_PARAM = "book_content like \"10%\"";
    List<String> outFields = Arrays.asList("book_name", "book_content");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(CommonData.defaultStringPKCollection)
            .withOutFields(outFields)
            .withConsistencyLevel(consistencyLevel)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    System.out.println(wrapperQuery.getFieldWrapper("book_name").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("book_content").getFieldData());
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_name").getFieldData().size() > 10);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("book_content").getFieldData().size() > 10);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Int PK and  float vector query without flush")
  public void intPKAndFloatVectorQueryWithoutFlush() {
    String newCollection = CommonFunction.createNewCollection();
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
    // query
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(newCollection)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
    // drop collection
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Test(description = "int PK and float vector query without pk field")
  @Severity(SeverityLevel.NORMAL)
  public void queryWithoutPKField() {
    String SEARCH_PARAM = "word_count in [10002,10004,10006,10008]";
    List<String> outFields = Arrays.asList("book_id", "word_count", CommonData.defaultVectorField);
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withOutFields(outFields)
                    .withExpr(SEARCH_PARAM)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    System.out.println(wrapperQuery.getFieldWrapper("book_id").getFieldData());
    System.out.println(wrapperQuery.getFieldWrapper("word_count").getFieldData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("word_count").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("book_id").getFieldData().size(), 4);
    Assert.assertEquals(wrapperQuery.getFieldWrapper(CommonData.defaultVectorField).getDim(), 128);
  }

  @Test(description = "int PK and float vector query with invalid expression")
  @Severity(SeverityLevel.NORMAL)
  public void queryWithInvalidExpression() {
    String SEARCH_PARAM = "word_count = 10002 ";
    List<String> outFields = Arrays.asList("book_id", "word_count", CommonData.defaultVectorField);
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withOutFields(outFields)
                    .withExpr(SEARCH_PARAM)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    Assert.assertTrue(queryResultsR.getException().getMessage().contains("cannot parse expression"));

  }

  @Test(description = "query collection with dynamic field",groups = {"Smoke"},dataProvider = "dynamicExpressions")
  @Severity(SeverityLevel.BLOCKER)
  public void queryCollectionWithDynamicField(String expr) {
    List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithDynamicField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(),0);
    CommonFunction.createIndexWithLoad(collectionWithDynamicField,IndexType.HNSW,MetricType.L2,CommonData.defaultVectorField);
    //query

    List<String> outFields = Collections.singletonList("extra_field");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithDynamicField)
                    .withOutFields(outFields)
                    .withExpr(expr)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
     Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
     Assert.assertTrue(wrapperQuery.getFieldWrapper("$meta").getFieldData().size()>=4);
    FieldDataWrapper fieldDataWrapper=new FieldDataWrapper(queryResultsR.getData().getFieldsData(0));
    String extra_field = fieldDataWrapper.getAsString(0, "extra_field");
    Assert.assertTrue(extra_field.contains("String"));
  }

  @Test(description = "query collection with dynamic field use inner json field",groups = {"Smoke"},dataProvider = "dynamicExpressions")
  @Severity(SeverityLevel.BLOCKER)
  public void queryWithDynamicFieldUseInnerJsonField(String expr) {
    List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithDynamicField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(),0);
    CommonFunction.createIndexWithLoad(collectionWithDynamicField,IndexType.HNSW,MetricType.L2,CommonData.defaultVectorField);
    //query
    List<String> outFields = Arrays.asList("json_field",CommonData.defaultVectorField);
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithDynamicField)
                    .withOutFields(outFields)
                    .withExpr(expr)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("$meta").getFieldData().size()>=4);
  }

  @Test(description = "query collection with dynamic field use nonexistent field name")
  @Severity(SeverityLevel.NORMAL)
  public void queryWithDynamicFieldUseNonexistentFiledName() {
    List<JSONObject> jsonObjects = CommonFunction.generateDataWithDynamicFiledRow(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithDynamicField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(),0);
    CommonFunction.createIndexWithLoad(collectionWithDynamicField,IndexType.HNSW,MetricType.L2,CommonData.defaultVectorField);
    //query
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("extra_field_nonexistent");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithDynamicField)
                    .withOutFields(outFields)
                    .withExpr(SEARCH_PARAM)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertEquals(wrapperQuery.getFieldWrapper("$meta").getFieldData().size(),4);
    FieldDataWrapper fieldDataWrapper=new FieldDataWrapper(queryResultsR.getData().getFieldsData(0));
    String extra_field = fieldDataWrapper.getAsString(0, "extra_field");
    Assert.assertTrue(extra_field.contains("String"));
  }

  @Test(description = "query collection with json field",groups = {"Smoke"},dataProvider = "jsonExpressions")
  @Severity(SeverityLevel.BLOCKER)
  public void queryCollectionWithJsonField(String expr) {
    List<JSONObject> jsonObjects = CommonFunction.generateJsonData(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithJsonField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(),0);
    CommonFunction.createIndexWithLoad(collectionWithJsonField,IndexType.HNSW,MetricType.L2,"float_vector");
    //query
    List<String> outFields = Arrays.asList("json_field");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithJsonField)
                    .withOutFields(outFields)
                    .withExpr(expr)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("json_field").getFieldData().size()>=4);
    JSONObject jsonObject = (JSONObject) wrapperQuery.getRowRecords().get(0).get("json_field");
    String string_field = jsonObject.getString("string_field");
    Assert.assertTrue(string_field.contains("Str"));
  }

  @Test(description = "query collection with array field",groups = {"Smoke"})
  @Severity(SeverityLevel.BLOCKER)
  public void queryCollectionWithArrayField() {
    List<JSONObject> jsonObjects = CommonFunction.generateJsonDataWithArrayField(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withRows(jsonObjects)
            .withCollectionName(collectionWithArrayField)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(),0);
    CommonFunction.createIndexWithLoad(collectionWithArrayField,IndexType.HNSW,MetricType.L2,"float_vector");
    //query
    List<String> outFields = Arrays.asList("str_array_field","int_array_field","float_array_field");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithArrayField)
                    .withOutFields(outFields)
//                    .withExpr(expr)
                    .withLimit(100L)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper("str_array_field").getFieldData().size()>=4);

  }

  @Test(description = "query collection with sparse vector",groups = {"Smoke"},dataProvider = "sparseIndex")
  public void queryCollectionWithSparseVector(IndexType indexType) {
    List<InsertParam.Field> fields = CommonFunction.generateDataWithSparseFloatVector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithSparseVector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    CommonFunction.createIndexWithLoad(collectionWithSparseVector, indexType, MetricType.IP, CommonData.defaultSparseVectorField);
    //query
    List<String> outFields = Collections.singletonList("*");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithSparseVector)
                    .withOutFields(outFields)
//                    .withExpr(expr)
                    .withLimit(100L).withOffset(100L)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper(CommonData.defaultSparseVectorField).getFieldData().size() >= 4);
  }

  @Test(description = "query collection with float16 vector",groups = {"Smoke"},dataProvider = "FloatIndex")
  public void queryCollectionWithFloat16Vector(IndexType indexType,MetricType metricType) {
    List<InsertParam.Field> fields = CommonFunction.generateDataWithFloat16Vector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithFloat16Vector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    CommonFunction.createIndexWithLoad(collectionWithFloat16Vector, indexType, metricType, CommonData.defaultFloat16VectorField);
    //query
    List<String> outFields = Collections.singletonList("*");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithFloat16Vector)
                    .withOutFields(outFields)
                    .withLimit(100L).withOffset(10L)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper(CommonData.defaultFloat16VectorField).getFieldData().size() >= 4);
  }

  @Test(description = "query collection with bf16 vector",groups = {"Smoke"},dataProvider = "FloatIndex")
  public void queryCollectionWithBF16Vector(IndexType indexType,MetricType metricType) {
    List<InsertParam.Field> fields = CommonFunction.generateDataWithBF16Vector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithBf16Vector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    CommonFunction.createIndexWithLoad(collectionWithBf16Vector, indexType, metricType, CommonData.defaultBF16VectorField);
    //query
    List<String> outFields = Collections.singletonList("*");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithBf16Vector)
                    .withOutFields(outFields)
                    .withLimit(100L).withOffset(10L)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper(CommonData.defaultBF16VectorField).getFieldData().size() >= 4);
  }


  @Test(description = "query collection with multi vector",groups = {"Smoke"})
  @Severity(SeverityLevel.BLOCKER)
  public void queryCollectionWithMultiVector() {
    List<InsertParam.Field> fields = CommonFunction.generateDataWithMultiVector(1000);
    R<MutationResult> insert = milvusClient.insert(InsertParam.newBuilder()
            .withCollectionName(collectionWithMultiVector)
            .withFields(fields)
            .build());
    Assert.assertEquals(insert.getStatus().intValue(), 0);
    CommonFunction.createIndexWithoutLoad(collectionWithMultiVector, IndexType.HNSW, MetricType.L2, CommonData.defaultVectorField);
    CommonFunction.createIndexWithoutLoad(collectionWithMultiVector, IndexType.BIN_FLAT, MetricType.HAMMING, CommonData.defaultBinaryVectorField);
    CommonFunction.createIndexWithoutLoad(collectionWithMultiVector, IndexType.SPARSE_INVERTED_INDEX, MetricType.IP, CommonData.defaultSparseVectorField);
    CommonFunction.createIndexWithoutLoad(collectionWithMultiVector, IndexType.HNSW, MetricType.L2, CommonData.defaultFloat16VectorField);

    R<RpcStatus> rpcStatusR = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
            .withCollectionName(collectionWithMultiVector)
            .withSyncLoad(true).build());
    System.out.println(rpcStatusR);
    //query
    List<String> outFields = Collections.singletonList("*");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(collectionWithMultiVector)
                    .withOutFields(outFields)
//                    .withExpr(expr)
                    .withLimit(100L).withOffset(100L)
                    .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    QueryResultsWrapper wrapperQuery = new QueryResultsWrapper(queryResultsR.getData());
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 0);
    Assert.assertTrue(wrapperQuery.getFieldWrapper(CommonData.defaultVectorField).getFieldData().size() >= 4);

  }


  }

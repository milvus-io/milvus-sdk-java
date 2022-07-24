package com.zilliz.milvustest.query;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DataType;
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
import java.util.List;

import static com.zilliz.milvustest.util.MathUtil.combine;

@Epic("Query")
@Feature("Query")
public class QueryTest extends BaseTest {
  public String newBookName;

  @BeforeClass(description = "load collection first")
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

  @DataProvider(name = "providerPartition")
  public Object[][] providerPartition() {
    return new Object[][] {{Boolean.FALSE}, {Boolean.TRUE}};
  }

  @AfterClass(description = "release collection after test")
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
      {IndexType.ANNOY},
      {IndexType.RHNSW_FLAT},
      {IndexType.RHNSW_PQ},
      {IndexType.RHNSW_SQ}
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
      {MetricType.JACCARD},
      {MetricType.SUBSTRUCTURE},
      {MetricType.SUPERSTRUCTURE},
      {MetricType.TANIMOTO}
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

  @Test(description = "int PK and float vector query", dataProvider = "providerPartition")
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
    boolean b =
        metricType.equals(MetricType.SUBSTRUCTURE) || metricType.equals(MetricType.SUPERSTRUCTURE);

    if (indexType.equals(IndexType.BIN_IVF_FLAT) && b) {
      return;
    }
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
    if (b) {
      return;
    }
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
}

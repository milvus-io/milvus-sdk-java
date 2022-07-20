package com.zilliz.milvustest.alias;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Epic("Alias")
@Feature("CreateAlias")
public class CreateAliasTest extends BaseTest {
  private String aliasStr;
  private String anotherAliasStr;

  @BeforeClass
  public void initData() {
    aliasStr = "alias_" + MathUtil.getRandomString(10);
    anotherAliasStr = "alias_" + MathUtil.getRandomString(10);
  }

  @AfterClass
  public void dropAlias() {
    milvusClient.dropAlias(DropAliasParam.newBuilder().withAlias(aliasStr).build());
    milvusClient.dropAlias(DropAliasParam.newBuilder().withAlias(anotherAliasStr).build());
  }

  @DataProvider(name = "provideAlias")
  public Object[][] provideAlias() {
    return new Object[][] {{aliasStr}, {anotherAliasStr}};
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create alias for collection")
  public void createAliasWithNonexistentCollection() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createAlias(
            CreateAliasParam.newBuilder()
                .withCollectionName("NonexistentCollection")
                .withAlias("alias"+MathUtil.getRandomString(10))
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(
        rpcStatusR.getException().getMessage().contains("aliased collection name does not exist"));
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Create alias for collection")
  public void createAliasTest() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createAlias(
            CreateAliasParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withAlias(aliasStr)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(
      description = "query collection with alias",
      dependsOnMethods = {"createAliasTest", "createAnotherAliasTest"},
      dataProvider = "provideAlias")
  public void queryWithAlias(String alias) {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName(alias).build());
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(alias)
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

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Create another alias for collection")
  public void createAnotherAliasTest() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createAlias(
            CreateAliasParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withAlias(anotherAliasStr)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(
          description = "search with alias",
          dependsOnMethods = {"createAliasTest", "createAnotherAliasTest"},
          dataProvider = "provideAlias")
  public void searchWithAlias(String alias) {
    milvusClient.loadCollection(
            LoadCollectionParam.newBuilder().withCollectionName(alias).build());
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(alias)
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
    Assert.assertEquals(searchResultsWrapper.getFieldData("book_id", 0).size(), SEARCH_K.intValue());
    System.out.println(searchResultsR.getData().getResults());
  }
}

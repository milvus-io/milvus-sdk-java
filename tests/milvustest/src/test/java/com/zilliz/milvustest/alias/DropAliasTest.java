package com.zilliz.milvustest.alias;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.DescCollResponseWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Epic("Alias")
@Feature("DropAlias")
public class DropAliasTest extends BaseTest {
  private String aliasStr;

  @BeforeClass(alwaysRun=true)
  public void initData() {
    aliasStr = "alias_" + MathUtil.getRandomString(10);
    milvusClient.createAlias(
        CreateAliasParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withAlias(aliasStr)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "drop alias",groups = {"Smoke"})
  public void dropAlias() {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropAlias(DropAliasParam.newBuilder().withAlias(aliasStr).build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "query after drop alias", dependsOnMethods = "dropAlias")
  public void queryAfterDropAlias() {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
        QueryParam.newBuilder()
            .withCollectionName(aliasStr)
            .withOutFields(outFields)
            .withExpr(SEARCH_PARAM)
            .build();
    R<QueryResults> queryResultsR = milvusClient.query(queryParam);
    Assert.assertEquals(queryResultsR.getStatus().intValue(), 1);
    Assert.assertEquals(
        queryResultsR.getException().getMessage(),"DescribeCollection failed: can't find collection: "+aliasStr);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "drop nonexistent alias")
  public void dropNonexistentAlias() {
    R<RpcStatus> rpcStatusR =
            milvusClient.dropAlias(DropAliasParam.newBuilder().withAlias("NonexistentAlias").build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(rpcStatusR.getException().getMessage().contains("alias does not exist"));
  }


  @Severity(SeverityLevel.NORMAL)
  @Test(description = "search after drop alias", dependsOnMethods = "dropAlias")
  public void searchAfterDropAlias() {
    milvusClient.loadCollection(
            LoadCollectionParam.newBuilder().withCollectionName(aliasStr).build());
    Integer SEARCH_K = 2; // TopK
    String SEARCH_PARAM = "{\"nprobe\":10}";
    List<String> search_output_fields = Arrays.asList("book_id");
    List<List<Float>> search_vectors = Arrays.asList(Arrays.asList(MathUtil.generateFloat(128)));
    SearchParam searchParam =
            SearchParam.newBuilder()
                    .withCollectionName(aliasStr)
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
            searchResultsR.getException().getMessage(),"DescribeCollection failed: can't find collection: "+aliasStr);
  }
  @Severity(SeverityLevel.NORMAL)
  @Test(description = "hasCollection after drop alias", dependsOnMethods = "dropAlias")
  public void hasCollectionAfterDropAlias() {
    R<Boolean> booleanR =
            milvusClient.hasCollection(
                    HasCollectionParam.newBuilder().withCollectionName(aliasStr).build());
    Assert.assertEquals(booleanR.getStatus().intValue(), 0);
    Assert.assertFalse(booleanR.getData());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Describe Collection after drop alias", dependsOnMethods = "dropAlias")
  public void describeCollectionAfterDropAlias() {
    R<DescribeCollectionResponse> describeCollectionResponseR =
            milvusClient.describeCollection(
                    DescribeCollectionParam.newBuilder().withCollectionName(aliasStr).build());
    Assert.assertEquals(describeCollectionResponseR.getStatus().intValue(), 1);
    Assert.assertTrue(describeCollectionResponseR.getException().getMessage().contains("can't find collection"));
  }
}

package com.zilliz.milvustest.alias;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.QueryResults;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.QueryParam;
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

  @BeforeClass
  public void initData() {
    aliasStr = "alias_" + MathUtil.getRandomString(10);
    milvusClient.createAlias(
        CreateAliasParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withAlias(aliasStr)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "drop alias")
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
}

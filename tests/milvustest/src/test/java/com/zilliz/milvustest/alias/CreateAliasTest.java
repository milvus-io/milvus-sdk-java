package com.zilliz.milvustest.alias;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.grpc.QueryResults;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.alias.DropAliasParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.control.GetQuerySegmentInfoParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.QueryResultsWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import lombok.NonNull;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Epic("Alias")
@Feature("CreateAlias")
public class CreateAliasTest extends BaseTest {
  private String aliasStr;

  @BeforeClass
  public void initData() {
    aliasStr = "alias_" + MathUtil.getRandomString(10);
  }

  @AfterClass
  public void dropAlias() {
    milvusClient.dropAlias(DropAliasParam.newBuilder().withAlias(aliasStr).build());
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
  @Test(description = "query collection with alias",dependsOnMethods = "createAliasTest")
  public void queryWithAlias() {
    milvusClient.loadCollection(LoadCollectionParam.newBuilder().withCollectionName(aliasStr).build());
    String SEARCH_PARAM = "book_id in [2,4,6,8]";
    List<String> outFields = Arrays.asList("book_id", "word_count");
    QueryParam queryParam =
            QueryParam.newBuilder()
                    .withCollectionName(aliasStr)
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

}

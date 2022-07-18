package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonFunction;
import com.zilliz.milvustest.util.MathUtil;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Collection")
@Feature("DropCollection")
public class DropCollectionTest extends BaseTest {
  public String commonCollection;

  @BeforeClass(description = "Create collection before test")
  public void provideCollectionName() {
    String newCollection = CommonFunction.createNewCollection();
    commonCollection = newCollection;
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "drop collection")
  public void dropCollectionSuccess() {
    DropCollectionParam build =
        DropCollectionParam.newBuilder().withCollectionName(commonCollection).build();
    R<RpcStatus> rpcStatusR = milvusClient.dropCollection(build);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    // TODO:校验load之后
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "drop illegal collection")
  public void dropIllegalCollection() {
    String collection = "collection_" + MathUtil.getRandomString(10);
    DropCollectionParam build =
        DropCollectionParam.newBuilder().withCollectionName(collection).build();
    R<RpcStatus> rpcStatusR = milvusClient.dropCollection(build);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertEquals(
        rpcStatusR.getException().getMessage(),
        "DescribeCollection failed: can't find collection: " + collection + "");
  }
}

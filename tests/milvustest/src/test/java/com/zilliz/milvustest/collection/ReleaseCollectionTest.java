package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Collection")
@Feature("ReleaseCollection")
public class ReleaseCollectionTest extends BaseTest {
  @BeforeClass(description = "load collection")
  public void loadCollection() {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "release collection")
  public void releaseCollectionTest() {
    R<RpcStatus> rpcStatusR =
        milvusClient.releaseCollection(
            ReleaseCollectionParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }
}

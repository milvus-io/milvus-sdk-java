package com.zilliz.milvustest.other;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import io.milvus.grpc.GetReplicasResponse;
import io.milvus.param.R;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.control.GetReplicasParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Replicas")
@Feature("GetReplicas")
public class GetReplicasTest extends BaseTest {
  @BeforeClass(description = "load collection first",alwaysRun = true)
  public void loadCollection() {
    milvusClient.loadCollection(
        LoadCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withReplicaNumber(2)
            .withSyncLoad(Boolean.FALSE)
            .withSyncLoadWaitingTimeout(30L)
            .withSyncLoadWaitingInterval(500L)
            .build());
  }

  @AfterClass(description = "release collection after test",alwaysRun = true)
  public void releaseCollection() {
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .build());
  }

  @Test(description = "Returns the collection's replica information",groups = {"Smoke"})
  @Severity(SeverityLevel.BLOCKER)
  public void getReplicasTest() {
    R<GetReplicasResponse> getReplicasResponseR =
        milvusClient.getReplicas(
            GetReplicasParam.newBuilder().withCollectionName(CommonData.defaultCollection).build());
    Assert.assertEquals(getReplicasResponseR.getStatus().intValue(), 0);
    Assert.assertTrue(getReplicasResponseR.getData().getReplicas(0).getReplicaID() > 0);
    System.out.println(getReplicasResponseR.getData());
  }
}

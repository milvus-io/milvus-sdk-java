package com.zilliz.milvustest.index;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.grpc.GetIndexStateResponse;
import io.milvus.grpc.IndexState;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.index.GetIndexStateParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Index")
@Feature("GetIndexState")
public class GetIndexStateTest extends BaseTest {
  public String collection;

  @BeforeClass(description = "Create collection and index for test")
  public void createCollectionAndIndex() {
    collection = CommonFunction.createNewCollection();
    R<RpcStatus> indexR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(collection)
                .withFieldName(CommonData.defaultVectorField)
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(MetricType.L2)
                .withIndexType(IndexType.IVF_FLAT)
                .withExtraParam(CommonData.defaultExtraParam)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build());
    softAssert.assertEquals(indexR.getStatus().intValue(), 0);
    softAssert.assertAll();
  }

  @AfterClass(description = "drop collection after test")
  public void dropCollection() {
    milvusClient.dropIndex(
        DropIndexParam.newBuilder()
            .withCollectionName(collection)
            .withIndexName(CommonData.defaultIndex)
            .build());
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Get index state ")
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/302")
  public void getIndexState() {
    R<GetIndexStateResponse> getIndexStateResponseR =
        milvusClient.getIndexState(
            GetIndexStateParam.newBuilder()
                .withCollectionName(collection)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(getIndexStateResponseR.getStatus().intValue(), 0);
    Assert.assertEquals(getIndexStateResponseR.getData().getState(), IndexState.Finished);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Get index state with nonexistent collection")
  public void getIndexStateWithNonexistentCollection() {
    R<GetIndexStateResponse> getIndexStateResponseR =
        milvusClient.getIndexState(
            GetIndexStateParam.newBuilder()
                .withCollectionName("NonexistentCollection")
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(getIndexStateResponseR.getStatus().intValue(), 1);
    Assert.assertTrue(getIndexStateResponseR.getException().getMessage().contains("not found"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Get index state when collection does not create index")
  public void getIndexStateWhenCollectionNotCreateIndex() {
    String newCollection = CommonFunction.createNewCollection();
    R<GetIndexStateResponse> getIndexStateResponseR =
        milvusClient.getIndexState(
            GetIndexStateParam.newBuilder()
                .withCollectionName(newCollection)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(getIndexStateResponseR.getStatus().intValue(), 1);
    Assert.assertTrue(
        getIndexStateResponseR.getException().getMessage().contains("index not found"));
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }
}

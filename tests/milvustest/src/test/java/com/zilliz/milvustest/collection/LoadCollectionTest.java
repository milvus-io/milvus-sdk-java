package com.zilliz.milvustest.collection;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.collection.ReleaseCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.partition.LoadPartitionsParam;
import io.milvus.param.partition.ReleasePartitionsParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

@Epic("Collection")
@Feature("LoadCollection")
public class LoadCollectionTest extends BaseTest {
  private String collection;

  @BeforeClass(description = "create new collection",alwaysRun=true)
  public void createCollection() {
    collection = CommonFunction.createNewCollection();
    // load need create index first
    milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                    .withCollectionName(collection)
                    .withFieldName(CommonData.defaultVectorField)
                    .withIndexName(CommonData.defaultIndex)
                    .withMetricType(MetricType.L2)
                    .withIndexType(IndexType.HNSW)
                    .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                    .withSyncMode(Boolean.TRUE)
                    .withSyncWaitingTimeout(30L)
                    .withSyncWaitingInterval(500L)
                    .build());
  }

  @AfterClass(description = "delete test collection",alwaysRun=true)
  public void dropCollection() {
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collection).build());
  }

  @AfterMethod(description = "release collection",alwaysRun=true)
  public void releaseCollection() {
    // release need drop index first
    milvusClient.dropIndex(DropIndexParam.newBuilder()
            .withCollectionName(collection)
            .withIndexName(CommonData.defaultIndex).build());
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder().withCollectionName(collection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "load collection with default param",groups = {"Smoke"})
  public void loadCollection() {
    LoadCollectionParam loadCollectionParam =
        new LoadCollectionParam(
            LoadCollectionParam.newBuilder()
                .withCollectionName(collection)
                .withSyncLoad(Boolean.TRUE)
                    .withSyncLoadWaitingTimeout(30L)
                    .withSyncLoadWaitingInterval(500L));
    R<RpcStatus> rpcStatusR = milvusClient.loadCollection(loadCollectionParam);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "load collection Async",groups = {"Smoke"})
  public void loadCollectionWithAsync() {
    LoadCollectionParam loadCollectionParam =
        LoadCollectionParam.newBuilder()
            .withCollectionName(collection)
            .withSyncLoad(Boolean.FALSE)
            .build();
    R<RpcStatus> rpcStatusR = milvusClient.loadCollection(loadCollectionParam);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Load partition after load collection")
  public void loadPartitionAfterLoadCollection() {
    LoadCollectionParam loadCollectionParam =
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build();
    milvusClient.loadCollection(loadCollectionParam);
    R<RpcStatus> rpcStatusR =
        milvusClient.loadPartitions(
            LoadPartitionsParam.newBuilder()
                .withCollectionName(CommonData.defaultCollection)
                .withPartitionNames(Arrays.asList(CommonData.defaultPartition))
                .build());

    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 5);
    Assert.assertTrue(
        rpcStatusR
            .getException()
            .getMessage()
            .contains("load the partition after load collection is not supported"));
    milvusClient.releaseCollection(
        ReleaseCollectionParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Load collection after load partition")
  public void loadCollectionAfterLoadPartition() {
    R<RpcStatus> loadPartitions = milvusClient.loadPartitions(
            LoadPartitionsParam.newBuilder()
                    .withCollectionName(CommonData.defaultCollection)
                    .withPartitionNames(Arrays.asList(CommonData.defaultPartition))
                    .build());
    LoadCollectionParam loadCollectionParam =
        LoadCollectionParam.newBuilder().withCollectionName(CommonData.defaultCollection).build();
    R<RpcStatus> rpcStatusR = milvusClient.loadCollection(loadCollectionParam);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 5);
    Assert.assertTrue(
        rpcStatusR
            .getException()
            .getMessage()
            .contains("load the partition after load collection is not supported"));
    milvusClient.releasePartitions(
        ReleasePartitionsParam.newBuilder()
            .withCollectionName(CommonData.defaultCollection)
            .withPartitionNames(Arrays.asList(CommonData.defaultPartition))
            .build());
  }


}

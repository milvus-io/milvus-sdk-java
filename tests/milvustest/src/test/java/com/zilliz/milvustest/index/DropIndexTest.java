package com.zilliz.milvustest.index;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Index")
@Feature("DropIndex")
public class DropIndexTest extends BaseTest {
  public String collection;

  @BeforeClass(description = "create collection and  index for test")
  public void createCollectionAndIndex() {
    collection = CommonFunction.createNewCollection();
    milvusClient.createIndex(
        CreateIndexParam.newBuilder()
            .withCollectionName(collection)
            .withFieldName(CommonData.defaultVectorField)
            .withIndexName(CommonData.defaultIndex)
            .withMetricType(MetricType.L2)
            .withIndexType(IndexType.IVF_FLAT)
            .withExtraParam(CommonData.defaultExtraParam)
            .withSyncMode(Boolean.FALSE)
            .build());
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
  @Test(description = "drop index success")
  public void dropIndexSuccess() {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropIndex(
            DropIndexParam.newBuilder()
                .withCollectionName(collection)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
  }

  @Severity(SeverityLevel.MINOR)
  @Test(description = "drop index without index name")
  public void dropIndexWithoutIndexName() {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropIndex(DropIndexParam.newBuilder().withCollectionName(collection).build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(),25);
    Assert.assertTrue(rpcStatusR.getException().getMessage().contains("Index doesn't exist"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "drop index with error index name")
  public void dropIndexWithErrorIndexName() {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropIndex(
            DropIndexParam.newBuilder()
                .withCollectionName(collection)
                .withIndexName("book_id")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 25);
    Assert.assertTrue(rpcStatusR.getException().getMessage().contains( "Index doesn't exist"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "drop index with nonexistent index name")
  public void dropIndexWithNonexistentIndexName() {
    R<RpcStatus> rpcStatusR =
        milvusClient.dropIndex(
            DropIndexParam.newBuilder()
                .withCollectionName(collection)
                .withIndexName("nonexistent")
                .build());
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 25);
    Assert.assertEquals(
        rpcStatusR.getException().getMessage(),
        "Index doesn't exist");
  }

  @Severity(SeverityLevel.MINOR)
  @Test(
      description = "describe index after drop index",
      dependsOnMethods = "dropIndexSuccess")
  public void describeIndexAfterDropIndex() {
    R<DescribeIndexResponse> describeIndexResponseR =
        milvusClient.describeIndex(
            DescribeIndexParam.newBuilder()
                .withCollectionName(collection)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 25);
    Assert.assertTrue(
            describeIndexResponseR.getException().getMessage().contains(
            "index not exist"));
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "create index after drop index", dependsOnMethods = {"dropIndexSuccess","describeIndexAfterDropIndex"})
  public void createIndexAfterDrop(){
    R<RpcStatus> indexR = milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                    .withCollectionName(collection)
                    .withFieldName(CommonData.defaultVectorField)
                    .withIndexName(CommonData.defaultIndex)
                    .withMetricType(MetricType.IP)
                    .withIndexType(IndexType.IVF_FLAT)
                    .withExtraParam(CommonData.defaultExtraParam)
                    .withSyncMode(Boolean.FALSE)
                    .build());
    Assert.assertEquals(indexR.getStatus().intValue(),0);
    Assert.assertEquals(indexR.getData().getMsg(),"Success");

  }
}

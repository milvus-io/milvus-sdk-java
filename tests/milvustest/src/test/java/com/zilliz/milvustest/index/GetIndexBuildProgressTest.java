package com.zilliz.milvustest.index;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.GetIndexBuildProgressResponse;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.param.index.GetIndexBuildProgressParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Index")
@Feature("GetIndexBuildProgress")
public class GetIndexBuildProgressTest extends BaseTest {
  public String collection;

  @BeforeClass(description = "Create collection and index for test")
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
            .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
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
  @Test(description = "Get Index Build Progress")
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/299")
  public void getIndexBuildProgressTest() {
    R<GetIndexBuildProgressResponse> indexBuildProgress =
        milvusClient.getIndexBuildProgress(
            GetIndexBuildProgressParam.newBuilder()
                .withCollectionName(collection)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(indexBuildProgress.getStatus().intValue(), 0);
    Assert.assertEquals(indexBuildProgress.getData().getIndexedRows(), 0);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Get Index Build Progress without index name")
  public void getIndexBuildProgressWithoutIndexName() {
    R<DescribeIndexResponse> describeIndexResponseR =
        milvusClient.describeIndex(
            DescribeIndexParam.newBuilder().withCollectionName(collection).build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 25);
    Assert.assertTrue(describeIndexResponseR.getException().getMessage().contains("index not exist"));
    }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Get Index Build Progress without collection",expectedExceptions = ParamException.class)
  public void getIndexBuildProgressWithoutCollection() {
    R<DescribeIndexResponse> describeIndexResponseR =
            milvusClient.describeIndex(
                    DescribeIndexParam.newBuilder().withIndexName(CommonData.defaultIndex).build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 1);
  }

}

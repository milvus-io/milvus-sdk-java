package com.zilliz.milvustest.index;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.exception.ParamException;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DescribeIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.milvus.response.DescIndexResponseWrapper;
import io.qameta.allure.Epic;
import io.qameta.allure.Feature;
import io.qameta.allure.Severity;
import io.qameta.allure.SeverityLevel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Epic("Index")
@Feature("DescribeIndex")
public class DescribeIndexTest extends BaseTest {
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
            .withSyncMode(Boolean.FALSE)
            .build());
  }

  @AfterClass(description = "drop collection after test")
  public void dropCollection() {
    milvusClient.dropIndex(
        DropIndexParam.newBuilder()
            .withCollectionName(collection)
            .withIndexName(CommonData.defaultVectorField)
            .build());
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collection).build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Test(description = "Describe Index")
  public void describeIndex() {
    R<DescribeIndexResponse> describeIndexResponseR =
        milvusClient.describeIndex(
            DescribeIndexParam.newBuilder()
                .withCollectionName(collection)
                .withIndexName(CommonData.defaultIndex)
                .build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 0);
    DescIndexResponseWrapper descIndexResponseWrapper =
        new DescIndexResponseWrapper(describeIndexResponseR.getData());
    Assert.assertEquals(
        descIndexResponseWrapper
            .getIndexDescByFieldName(CommonData.defaultVectorField)
            .getIndexName(),
        CommonData.defaultIndex);
    Assert.assertEquals(
        descIndexResponseWrapper
            .getIndexDescByFieldName(CommonData.defaultVectorField)
            .getFieldName(),
        CommonData.defaultVectorField);
  }

  @Severity(SeverityLevel.MINOR)
  @Test(
      description = "Describe Index without collection name",
      expectedExceptions = ParamException.class)
  public void describeIndexWithoutCollection() {
    R<DescribeIndexResponse> describeIndexResponseR =
        milvusClient.describeIndex(
            DescribeIndexParam.newBuilder().withIndexName(CommonData.defaultIndex).build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 0);
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Describe Index without index name")
  public void describeIndexWithoutIndexNam() {
    R<DescribeIndexResponse> describeIndexResponseR =
        milvusClient.describeIndex(
            DescribeIndexParam.newBuilder()
                .withCollectionName(collection)
                .build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 25);
    Assert.assertTrue(describeIndexResponseR.getException().getMessage().contains("index not exist"));
    }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Describe Index when collection does not create index")
  public void describeIndexWhenCollectionNotCreateIndex() {
    String newCollection = CommonFunction.createNewCollection();
    R<DescribeIndexResponse> describeIndexResponseR =
            milvusClient.describeIndex(
                    DescribeIndexParam.newBuilder()
                            .withCollectionName(newCollection)
                            .build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 25);
    Assert.assertTrue(describeIndexResponseR.getException().getMessage().contains("index not exist"));
    milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Describe Index with nonexistent collection")
  public void describeIndexWithNonexistentCollection() {
    R<DescribeIndexResponse> describeIndexResponseR =
            milvusClient.describeIndex(
                    DescribeIndexParam.newBuilder()
                            .withCollectionName("NonexistentCollection")
                            .build());
    Assert.assertEquals(describeIndexResponseR.getStatus().intValue(), 1);
    Assert.assertTrue(describeIndexResponseR.getException().getMessage().contains("not found"));
  }

}

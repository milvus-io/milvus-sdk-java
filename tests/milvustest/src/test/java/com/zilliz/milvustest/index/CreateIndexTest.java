package com.zilliz.milvustest.index;

import com.zilliz.milvustest.common.BaseTest;
import com.zilliz.milvustest.common.CommonData;
import com.zilliz.milvustest.common.CommonFunction;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.DropIndexParam;
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.zilliz.milvustest.util.MathUtil.combine;

@Epic("Index")
@Feature("CreateIndex")
public class CreateIndexTest extends BaseTest {
  public String collection;
  public String binaryCollection;

  @BeforeClass(description = "create collection for test")
  public void createCollection() {
    collection = CommonFunction.createNewCollection();
    binaryCollection = CommonFunction.createBinaryCollection();
  }

  @AfterClass(description = "drop collection after test")
  public void dropCollection() {
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(collection).build());
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(binaryCollection).build());
  }

  @DataProvider(name = "IndexTypes")
  public Object[][] provideIndexType() {
    return new Object[][] {
      {IndexType.FLAT},
      {IndexType.IVF_FLAT},
      {IndexType.IVF_SQ8},
      {IndexType.IVF_PQ},
      {IndexType.HNSW},
      {IndexType.ANNOY},
      {IndexType.RHNSW_FLAT},
      {IndexType.RHNSW_PQ},
      {IndexType.RHNSW_SQ}
    };
  }

  @DataProvider(name = "BinaryIndexTypes")
  public Object[][] provideBinaryIndexType() {
    return new Object[][] {{IndexType.BIN_IVF_FLAT}, {IndexType.BIN_FLAT}};
  }

  @DataProvider(name = "MetricType")
  public Object[][] providerMetricType() {
    return new Object[][] {{MetricType.L2}, {MetricType.IP}};
  }

  @DataProvider(name = "BinaryMetricType")
  public Object[][] providerBinaryMetricType() {
    return new Object[][] {
      {MetricType.HAMMING},
      {MetricType.JACCARD},
      {MetricType.SUBSTRUCTURE},
      {MetricType.SUPERSTRUCTURE},
      {MetricType.TANIMOTO}
    };
  }

  @DataProvider(name = "FloatIndex")
  public Object[][] providerIndexForFloatCollection() {
    return combine(provideIndexType(), providerMetricType());
  }

  @DataProvider(name = "BinaryIndex")
  public Object[][] providerIndexForBinaryCollection() {
    return combine(provideBinaryIndexType(), providerBinaryMetricType());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/311")
  @Test(description = "Create index for collection sync", dataProvider = "FloatIndex")
  public void createIndexSync(IndexType indexType, MetricType metricType) {
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(collection)
                .withFieldName(CommonData.defaultVectorField)
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(metricType)
                .withIndexType(indexType)
                .withExtraParam(CommonFunction.provideExtraParam(indexType))
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropIndex(
        DropIndexParam.newBuilder()
            .withCollectionName(collection)
            .withIndexName(CommonData.defaultIndex)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/311")
  @Test(description = "Create index for collection Async", dataProvider = "FloatIndex")
  public void createIndexAsync(IndexType indexType, MetricType metricType) {
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(collection)
                .withFieldName(CommonData.defaultVectorField)
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(metricType)
                .withIndexType(indexType)
                    .withExtraParam(CommonFunction.provideExtraParam(indexType))
                .withSyncMode(Boolean.FALSE)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropIndex(
        DropIndexParam.newBuilder()
            .withCollectionName(collection)
            .withIndexName(CommonData.defaultIndex)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/321")
  @Test(description = "Create index for collection Async", dataProvider = "BinaryIndex")
  public void createBinaryIndexAsync(IndexType indexType, MetricType metricType) {
    if(indexType.equals(IndexType.BIN_IVF_FLAT)&&(metricType.equals(MetricType.SUBSTRUCTURE)||metricType.equals(MetricType.SUPERSTRUCTURE))){
      return;
    }
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(binaryCollection)
                .withFieldName(CommonData.defaultBinaryVectorField)
                .withIndexName(CommonData.defaultBinaryIndex)
                .withMetricType(metricType)
                .withIndexType(indexType)
                .withExtraParam(CommonData.defaultExtraParam)
                .withSyncMode(Boolean.FALSE)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropIndex(
        DropIndexParam.newBuilder()
            .withCollectionName(binaryCollection)
            .withIndexName(CommonData.defaultBinaryIndex)
            .build());
  }

  @Severity(SeverityLevel.BLOCKER)
  @Issue("https://github.com/milvus-io/milvus-sdk-java/issues/321")
  @Test(description = "Create index for collection sync", dataProvider = "BinaryIndex")
  public void createBinaryIndexSync(IndexType indexType, MetricType metricType) {
    if(indexType.equals(IndexType.BIN_IVF_FLAT)&&(metricType.equals(MetricType.SUBSTRUCTURE)||metricType.equals(MetricType.SUPERSTRUCTURE))){
      return;
    }
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(binaryCollection)
                .withFieldName(CommonData.defaultBinaryVectorField)
                .withIndexName(CommonData.defaultBinaryIndex)
                .withMetricType(metricType)
                .withIndexType(indexType)
                .withExtraParam(CommonData.defaultExtraParam)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropIndex(
        DropIndexParam.newBuilder()
            .withCollectionName(binaryCollection)
            .withIndexName(CommonData.defaultBinaryIndex)
            .build());
  }
}

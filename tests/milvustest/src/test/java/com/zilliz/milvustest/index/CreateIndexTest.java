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

  @BeforeClass(description = "create collection for test",alwaysRun = true)
  public void createCollection() {
    collection = CommonFunction.createNewCollection();
    binaryCollection = CommonFunction.createBinaryCollection();
  }

  @AfterClass(description = "drop collection after test",alwaysRun = true)
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
  @Test(description = "Create index for collection sync", dataProvider = "FloatIndex",groups = {"Smoke"})
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
    if (indexType.equals(IndexType.BIN_IVF_FLAT)
        && (metricType.equals(MetricType.SUBSTRUCTURE)
            || metricType.equals(MetricType.SUPERSTRUCTURE))) {
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
    if (indexType.equals(IndexType.BIN_IVF_FLAT)
        && (metricType.equals(MetricType.SUBSTRUCTURE)
            || metricType.equals(MetricType.SUPERSTRUCTURE))) {
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

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Float collection create index with error metric type")
  public void createIndexWithErrorMetricType() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(collection)
                .withFieldName(CommonData.defaultVectorField)
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(MetricType.JACCARD)
                .withIndexType(IndexType.HNSW)
                .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(rpcStatusR.getException().getMessage().contains("invalid index params"));
    milvusClient.dropIndex(
        DropIndexParam.newBuilder()
            .withCollectionName(collection)
            .withIndexName(CommonData.defaultIndex)
            .build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create index with nonexistent collection")
  public void createIndexWithNonexistentCollection() {
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName("NonexistentCollection")
                .withFieldName(CommonData.defaultVectorField)
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(MetricType.IP)
                .withIndexType(IndexType.HNSW)
                .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(rpcStatusR.getException().getMessage().contains("can't find collection"));
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Create index with scalar field")
  public void createIndexWithScalarField() {
    String stringPKCollection = CommonFunction.createStringPKCollection();
    R<RpcStatus> rpcStatusR =
        milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                .withCollectionName(stringPKCollection)
                .withFieldName("book_content")
                .withIndexName(CommonData.defaultIndex)
                .withMetricType(MetricType.IP)
                .withIndexType(IndexType.HNSW)
                .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropCollection(
        DropCollectionParam.newBuilder().withCollectionName(stringPKCollection).build());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Create index with String PK")
  public void createIndexWithStringPK() {
    String stringPKCollection = CommonFunction.createStringPKCollection();
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(stringPKCollection)
                            .withFieldName("book_name")
                            .withIndexName(CommonData.defaultIndex)
                            .withMetricType(MetricType.IP)
                            .withIndexType(IndexType.HNSW)
                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingTimeout(30L)
                            .withSyncWaitingInterval(500L)
                            .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(stringPKCollection).build());
  }

  @Severity(SeverityLevel.CRITICAL)
  @Test(description = "Create index with int PK")
  public void createIndexWithintPK() {
    String newCollection = CommonFunction.createNewCollection();
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(newCollection)
                            .withFieldName("book_id")
                            .withIndexName(CommonData.defaultIndex)
                            .withMetricType(MetricType.IP)
                            .withIndexType(IndexType.HNSW)
                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingTimeout(30L)
                            .withSyncWaitingInterval(500L)
                            .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "empty collection create index")
  public void emptyCollectionCreateIndex() {
    String newCollection = CommonFunction.createNewCollection();
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(newCollection)
                            .withFieldName(CommonData.defaultVectorField)
                            .withIndexName(CommonData.defaultIndex)
                            .withMetricType(MetricType.IP)
                            .withIndexType(IndexType.HNSW)
                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingTimeout(30L)
                            .withSyncWaitingInterval(500L)
                            .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(newCollection).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "repeat create index")
  public void repeatCreateIndex() {
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(CommonData.defaultCollection)
                            .withFieldName(CommonData.defaultVectorField)
                            .withIndexName(CommonData.defaultIndex)
                            .withMetricType(MetricType.IP)
                            .withIndexType(IndexType.HNSW)
                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingTimeout(30L)
                            .withSyncWaitingInterval(500L)
                            .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    Assert.assertTrue(rpcStatusR.getException().getMessage().contains("index already exists"));

  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create multiple index with different fields")
  public void createMultiIndexWithDiffFields() {
    String stringPKCollection = CommonFunction.createStringPKCollection();
    milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                    .withCollectionName(stringPKCollection)
                    .withFieldName("book_content")
                    .withIndexName("indexName1")
                    .withMetricType(MetricType.IP)
                    .withIndexType(IndexType.HNSW)
                    .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                    .withSyncMode(Boolean.TRUE)
                    .withSyncWaitingTimeout(30L)
                    .withSyncWaitingInterval(500L)
                    .build());
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(stringPKCollection)
                            .withFieldName("book_name")
                            .withIndexName(CommonData.defaultIndex)
                            .withMetricType(MetricType.IP)
                            .withIndexType(IndexType.HNSW)
                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingTimeout(30L)
                            .withSyncWaitingInterval(500L)
                            .build());
    System.out.println("Create index" + rpcStatusR);
    Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
    Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(stringPKCollection).build());
  }

  @Severity(SeverityLevel.NORMAL)
  @Test(description = "Create multiple index with same index name")
  public void createMultiIndexWithSameIndexName() {
    String stringPKCollection = CommonFunction.createStringPKCollection();
    milvusClient.createIndex(
            CreateIndexParam.newBuilder()
                    .withCollectionName(stringPKCollection)
                    .withFieldName("book_content")
                    .withIndexName(CommonData.defaultIndex)
                    .withMetricType(MetricType.IP)
                    .withIndexType(IndexType.HNSW)
                    .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                    .withSyncMode(Boolean.TRUE)
                    .withSyncWaitingTimeout(30L)
                    .withSyncWaitingInterval(500L)
                    .build());
    R<RpcStatus> rpcStatusR =
            milvusClient.createIndex(
                    CreateIndexParam.newBuilder()
                            .withCollectionName(stringPKCollection)
                            .withFieldName("book_name")
                            .withIndexName(CommonData.defaultIndex)
                            .withMetricType(MetricType.IP)
                            .withIndexType(IndexType.HNSW)
                            .withExtraParam(CommonFunction.provideExtraParam(IndexType.HNSW))
                            .withSyncMode(Boolean.TRUE)
                            .withSyncWaitingTimeout(30L)
                            .withSyncWaitingInterval(500L)
                            .build());
    System.out.println("Create index" + rpcStatusR);
    softAssert.assertEquals(rpcStatusR.getStatus().intValue(), 1);
    softAssert.assertTrue(rpcStatusR.getException().getMessage().contains("index already exists"));
    milvusClient.dropCollection(
            DropCollectionParam.newBuilder().withCollectionName(stringPKCollection).build());
    softAssert.assertAll();
  }


}

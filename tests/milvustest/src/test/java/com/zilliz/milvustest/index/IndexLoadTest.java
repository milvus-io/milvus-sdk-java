package com.zilliz.milvustest.index;

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
import io.qameta.allure.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.zilliz.milvustest.util.MathUtil.combine;

@Epic("Index")
@Feature("IndexLoad")
public class IndexLoadTest extends BaseTest {
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
                {IndexType.IVF_FLAT},
                {IndexType.IVF_SQ8},
                {IndexType.IVF_PQ},
                {IndexType.HNSW},
                {IndexType.SCANN},
                {IndexType.GPU_IVF_FLAT},
                {IndexType.GPU_IVF_PQ}
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
                {MetricType.JACCARD}
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

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Test create/drop index when collection is loaded for float vector", dataProvider = "FloatIndex",groups = {"Smoke"})
    public void createIndexAfterLoadFloatCollection(IndexType indexType, MetricType metricType) {
        // 1. create index params
        CreateIndexParam createIndexParams = CreateIndexParam.newBuilder()
                .withCollectionName(collection)
                .withFieldName(CommonData.defaultVectorField)
                .withMetricType(metricType)
                .withIndexType(indexType)
                .withExtraParam(CommonFunction.provideExtraParam(indexType))
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build();
        // 2. create index
        R<RpcStatus> rpcStatusR = milvusClient.createIndex(createIndexParams);
        System.out.println("Create index" + rpcStatusR);
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");

        // 3. load collection
        R<RpcStatus> rpcStatusR2 = milvusClient.loadCollection(
                LoadCollectionParam.newBuilder()
                .withCollectionName(collection)
                .withSyncLoad(Boolean.TRUE)
                .build());
        System.out.println("Load collection " + rpcStatusR);
        Assert.assertEquals(rpcStatusR2.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR2.getData().getMsg(), "Success");

      /*  // 4. create index
        R<RpcStatus> rpcStatusR3 = milvusClient.createIndex(createIndexParams);
        System.out.println("Create index " + rpcStatusR);
        Assert.assertEquals(rpcStatusR3.getStatus().intValue(), 1);
        Assert.assertTrue(rpcStatusR3.getMessage().contains("create index failed, collection is loaded, please release it first"));
*/
        // 5. drop index
        R<RpcStatus> rpcStatusR4 = milvusClient.dropIndex(
                        DropIndexParam.newBuilder()
                                .withCollectionName(collection)
                                .build());
        System.out.println("Drop index " + rpcStatusR4);
        Assert.assertEquals(rpcStatusR4.getStatus().intValue(), 65535);
        Assert.assertTrue(rpcStatusR4.getMessage().contains("index cannot be dropped, collection is loaded, please release it first"));

        // 6. release collection
        R<RpcStatus> rpcStatusR5 = milvusClient.releaseCollection(
                ReleaseCollectionParam.newBuilder()
                        .withCollectionName(collection)
                        .build());
        System.out.println("Release collection " + rpcStatusR);
        Assert.assertEquals(rpcStatusR5.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR5.getData().getMsg(), "Success");

        // 7. create index
        R<RpcStatus> rpcStatusR6 =
                milvusClient.createIndex(createIndexParams);
        System.out.println("Create index " + rpcStatusR);
        Assert.assertEquals(rpcStatusR6.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR6.getData().getMsg(), "Success");

        // 8. drop index
        R<RpcStatus> rpcStatusR7 = milvusClient.dropIndex(
                DropIndexParam.newBuilder()
                        .withCollectionName(collection)
                        .build());
        System.out.println("Drop index " + rpcStatusR);
        Assert.assertEquals(rpcStatusR7.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR7.getData().getMsg(), "Success");
    }

    @Severity(SeverityLevel.NORMAL)
    @Test(description = "Test create/drop index when collection is loaded for binary vector",
            dataProvider = "BinaryIndex",groups = {"Smoke"})
    public void createIndexAfterLoadBinaryCollection(IndexType indexType, MetricType metricType) {
        // 1. create index params
        CreateIndexParam createIndexParams = CreateIndexParam.newBuilder()
                .withCollectionName(binaryCollection)
                .withFieldName(CommonData.defaultBinaryVectorField)
                .withMetricType(metricType)
                .withIndexType(indexType)
                .withExtraParam(CommonFunction.provideExtraParam(indexType))
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingTimeout(30L)
                .withSyncWaitingInterval(500L)
                .build();
        // 2. create index
        R<RpcStatus> rpcStatusR = milvusClient.createIndex(createIndexParams);
        System.out.println("Create index" + rpcStatusR);
        Assert.assertEquals(rpcStatusR.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR.getData().getMsg(), "Success");

        // 3. load collection
        R<RpcStatus> rpcStatusR2 = milvusClient.loadCollection(
                LoadCollectionParam.newBuilder()
                        .withCollectionName(binaryCollection)
                        .withSyncLoad(Boolean.TRUE)
                        .build());
        System.out.println("Load collection " + rpcStatusR);
        Assert.assertEquals(rpcStatusR2.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR2.getData().getMsg(), "Success");

      /*  // 4. create index
        R<RpcStatus> rpcStatusR3 = milvusClient.createIndex(createIndexParams);
        System.out.println("Create index " + rpcStatusR);
        Assert.assertEquals(rpcStatusR3.getStatus().intValue(), 1);
        Assert.assertTrue(rpcStatusR3.getMessage().contains("create index failed, collection is loaded, please release it first"));
*/
        // 5. drop index
        R<RpcStatus> rpcStatusR4 = milvusClient.dropIndex(
                DropIndexParam.newBuilder()
                        .withCollectionName(binaryCollection)
                        .build());
        System.out.println("Drop index " + rpcStatusR4);
        Assert.assertEquals(rpcStatusR4.getStatus().intValue(), 65535);
        Assert.assertTrue(rpcStatusR4.getMessage().contains("index cannot be dropped, collection is loaded, please release it first"));

        // 6. release collection
        R<RpcStatus> rpcStatusR5 = milvusClient.releaseCollection(
                ReleaseCollectionParam.newBuilder()
                        .withCollectionName(binaryCollection)
                        .build());
        System.out.println("Release collection " + rpcStatusR);
        Assert.assertEquals(rpcStatusR5.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR5.getData().getMsg(), "Success");

        // 7. create index
        R<RpcStatus> rpcStatusR6 =
                milvusClient.createIndex(createIndexParams);
        System.out.println("Create index " + rpcStatusR);
        Assert.assertEquals(rpcStatusR6.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR6.getData().getMsg(), "Success");

        // 8. drop index
        R<RpcStatus> rpcStatusR7 = milvusClient.dropIndex(
                DropIndexParam.newBuilder()
                        .withCollectionName(binaryCollection)
                        .build());
        System.out.println("Drop index " + rpcStatusR);
        Assert.assertEquals(rpcStatusR7.getStatus().intValue(), 0);
        Assert.assertEquals(rpcStatusR7.getData().getMsg(), "Success");
    }
}

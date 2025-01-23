package com.zilliz.milvustestv2.partition;

import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.GetPartitionStatsReq;
import io.milvus.v2.service.partition.response.GetPartitionStatsResp;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class GetPartitionStatsTest extends BaseTest {

    String floatVectorCollection;
    String sparseFloatVectorCollection;
    String float16VectorCollection;
    String bFloat16VectorCollection;
    String binaryVectorCollection;

    @DataProvider(name = "initCollection")
    public Object[][] providerCollection() {
        floatVectorCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(floatVectorCollection)
                .partitionName(CommonData.partitionName)
                .build());

        sparseFloatVectorCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.SparseFloatVector);
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(sparseFloatVectorCollection)
                .partitionName(CommonData.partitionName)
                .build());
        float16VectorCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.Float16Vector);
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(float16VectorCollection)
                .partitionName(CommonData.partitionName)
                .build());
        bFloat16VectorCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.BFloat16Vector);
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(bFloat16VectorCollection)
                .partitionName(CommonData.partitionName)
                .build());
        binaryVectorCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.BinaryVector);
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(binaryVectorCollection)
                .partitionName(CommonData.partitionName)
                .build());
        return new Object[][]{
                {floatVectorCollection},
                {sparseFloatVectorCollection},
                {float16VectorCollection},
                {bFloat16VectorCollection},
                {binaryVectorCollection},
        };
    }
    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(floatVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(sparseFloatVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(float16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(bFloat16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(binaryVectorCollection).build());
    }

    @Test(description = "get partition stats",dataProvider = "initCollection")
    public void getPartitionStatsTest(String collection) throws InterruptedException {
        // insert
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(collection)
                .partitionName(CommonData.partitionName)
                .data(CommonFunction.genCommonData(collection, CommonData.numberEntities)).build());
        milvusClientV2.flush(FlushReq.builder().collectionNames(Lists.newArrayList(collection)).build());
        GetPartitionStatsResp partitionStats = milvusClientV2.getPartitionStats(GetPartitionStatsReq.builder()
                .collectionName(collection)
                .partitionName(CommonData.partitionName)
                .build());
        Assert.assertEquals(partitionStats.getNumOfEntities(),CommonData.numberEntities);

    }
}

package com.zilliz.milvustestv2.partition;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import org.checkerframework.checker.units.qual.A;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:35
 */
public class ListPartitionsTest extends BaseTest {
    String floatVectorCollection;
    String sparseFloatVectorCollection;
    String float16VectorCollection;
    String bFloat16VectorCollection;
    String binaryVectorCollection;

    @DataProvider(name = "initCollection")
    public Object[][] providerCollection() {
        floatVectorCollection = CommonFunction.createNewCollection(128, null, DataType.FloatVector);
        sparseFloatVectorCollection = CommonFunction.createNewCollection(128, null, DataType.SparseFloatVector);
        float16VectorCollection = CommonFunction.createNewCollection(128, null, DataType.Float16Vector);
        bFloat16VectorCollection = CommonFunction.createNewCollection(128, null, DataType.BFloat16Vector);
        binaryVectorCollection = CommonFunction.createNewCollection(128, null, DataType.BinaryVector);
        return new Object[][]{
                {floatVectorCollection},
                {sparseFloatVectorCollection},
                {float16VectorCollection},
                {bFloat16VectorCollection},
                {binaryVectorCollection},
        };
    }
    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(floatVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(sparseFloatVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(float16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(bFloat16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(binaryVectorCollection).build());

    }
    @Test(description = "List partitions", groups = {"Smoke"},dataProvider = "initCollection")
    public void listPartitions(String collectionName) {
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(collectionName)
                .partitionName(CommonData.partitionName)
                .build());
        List<String> strings = milvusClientV2.listPartitions(ListPartitionsReq.builder().collectionName(collectionName).build());
        Assert.assertTrue(strings.contains(CommonData.partitionName));
    }

    @Test(description = "List partitions", groups = {"Smoke"})
    public void listPartitions2() {
        List<String> strings = milvusClientV2.listPartitions(ListPartitionsReq.builder().collectionName(CommonData.defaultFloatVectorCollection).build());
        Assert.assertEquals(strings.size(),4);
        Assert.assertTrue(strings.contains("_default"));
        Assert.assertTrue(strings.contains(CommonData.partitionNameA));
        Assert.assertTrue(strings.contains(CommonData.partitionNameB));
        Assert.assertTrue(strings.contains(CommonData.partitionNameC));
    }

}

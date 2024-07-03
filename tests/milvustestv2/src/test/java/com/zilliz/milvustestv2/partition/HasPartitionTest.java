package com.zilliz.milvustestv2.partition;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:35
 */
public class HasPartitionTest extends BaseTest {
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
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(floatVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(sparseFloatVectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(float16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(bFloat16VectorCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(binaryVectorCollection).build());

    }
    @Test(description = "Has partition", groups = {"Smoke"},dataProvider = "initCollection")
    public void hasPartition(String collectionName) {
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(collectionName)
                .partitionName(CommonData.partitionName)
                .build());
        Boolean aBoolean = milvusClientV2.hasPartition(HasPartitionReq.builder()
                .collectionName(collectionName)
                .partitionName(CommonData.partitionName)
                .build());
        Assert.assertTrue(aBoolean);
    }

}

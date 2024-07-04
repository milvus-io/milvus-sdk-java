package com.zilliz.milvustestv2.partition;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:33
 */
public class CreatePartitionTest extends BaseTest {
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

    @Test(description = "Create partition", groups = {"Smoke"},dataProvider = "initCollection")
    public void createPartition(String newCollection) {
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(newCollection)
                .partitionName(CommonData.partitionName)
                .build());
        Boolean aBoolean = milvusClientV2.hasPartition(HasPartitionReq.builder()
                .collectionName(newCollection)
                .partitionName(CommonData.partitionName)
                .build());
        Assert.assertTrue(aBoolean);

    }
}

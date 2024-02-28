package com.zilliz.milvustestv2.partition;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:35
 */
public class HasPartitionTest extends BaseTest {
    String newCollection;
    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollection = CommonFunction.createNewCollection(128, null);
        milvusClientV2.createPartition(CreatePartitionReq.builder()
                .collectionName(newCollection)
                .partitionName(CommonData.partitionName)
                .build());
    }
    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollection).build());
    }
    @Test(description = "Has partition", groups = {"Smoke"})
    public void hasPartition() {
        Boolean aBoolean = milvusClientV2.hasPartition(HasPartitionReq.builder()
                .collectionName(newCollection)
                .partitionName(CommonData.partitionName)
                .build());
        Assert.assertTrue(aBoolean);
    }

}

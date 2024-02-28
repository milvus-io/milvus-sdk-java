package com.zilliz.milvustestv2.partition;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import org.checkerframework.checker.units.qual.A;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:35
 */
public class ListPartitionsTest extends BaseTest {
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

    @Test(description = "List partitions", groups = {"Smoke"})
    public void listPartitions() {
        List<String> strings = milvusClientV2.listPartitions(ListPartitionsReq.builder().collectionName(newCollection).build());
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

package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/1/31 15:24
 */

public class CreateCollectionTest extends BaseTest {
    String simpleCollection="simpleCollection";
    String repeatCollection="repeatCollection";

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(simpleCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(repeatCollection).build());

    }

    @Test(description = "Create simple collection success", groups = {"Smoke"})
    public void createSimpleCollectionSuccess(){
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                        .collectionName(simpleCollection)
                        .dimension(CommonData.dim)
                        .autoID(false)
                .build());
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains("simpleCollection"));
    }

    @Test(description = "Create duplicate collection", groups = {"Smoke"})
    public void createDuplicateSimpleCollection(){
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(repeatCollection)
                .dimension(CommonData.dim)
                .autoID(true)
                .build());
        try {
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(repeatCollection)
                    .dimension(CommonData.dim+1)
                    .autoID(true)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("create duplicate collection with different parameters"));
        }
    }

}

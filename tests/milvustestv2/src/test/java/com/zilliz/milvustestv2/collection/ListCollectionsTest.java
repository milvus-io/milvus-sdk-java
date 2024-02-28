package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:22
 */
public class ListCollectionsTest extends BaseTest {
    @Test(description = "List collections", groups = {"Smoke"})
    public void listCollection(){
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertTrue(listCollectionsResp.getCollectionNames().contains(CommonData.defaultFloatVectorCollection));
    }
}

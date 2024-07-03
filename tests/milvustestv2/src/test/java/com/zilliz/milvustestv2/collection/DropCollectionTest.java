package com.zilliz.milvustestv2.collection;

import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 11:41
 */
public class DropCollectionTest extends BaseTest {

    @DataProvider(name = "initCollection")
    public Object[][] providerCollection(){
        String newCollection = CommonFunction.createNewCollection(128, null, DataType.FloatVector);
        return new Object[][]{{newCollection}};
    }

    @Test(description = "Describe collection", groups = {"Smoke"},dataProvider = "initCollection")
    public void dropCollection(String collectionName){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        ListCollectionsResp listCollectionsResp = milvusClientV2.listCollections();
        Assert.assertFalse(listCollectionsResp.getCollectionNames().contains(collectionName));
    }



}

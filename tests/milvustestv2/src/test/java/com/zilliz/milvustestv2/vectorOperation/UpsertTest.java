package com.zilliz.milvustestv2.vectorOperation;

import com.alibaba.fastjson.JSONObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.UpsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:02
 */
public class UpsertTest extends BaseTest {
    String newCollectionName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null);
        List<JSONObject> jsonObjects = CommonFunction.generateDefaultData(CommonData.numberEntities, CommonData.dim);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "upsert collection",groups = {"Smoke"})
    public void upsert(){
        List<JSONObject> jsonObjects = CommonFunction.generateDefaultData(10, CommonData.dim);
        UpsertResp upsert = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(newCollectionName)
                .data(jsonObjects)
                .build());
        Assert.assertEquals(upsert.getUpsertCnt(),10);

    }

    @Test(description = "upsert collection",groups = {"Smoke"})
    public void simpleUpsert(){
        String collection=CommonFunction.createSimpleCollection(128,null);
        List<JSONObject> jsonObjects = CommonFunction.generateSimpleData(CommonData.numberEntities, CommonData.dim);
        milvusClientV2.insert(InsertReq.builder().collectionName(collection).data(jsonObjects).build());
        List<JSONObject> jsonObjectsNew = CommonFunction.generateSimpleData(10, CommonData.dim);
        UpsertResp upsert = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collection)
                .data(jsonObjectsNew)
                .build());
        System.out.println(upsert);
    }

}

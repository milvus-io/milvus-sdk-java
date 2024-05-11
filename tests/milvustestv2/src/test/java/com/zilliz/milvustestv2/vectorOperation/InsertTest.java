package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:02
 */
public class InsertTest extends BaseTest {
    String newCollectionName;
    String simpleCollectionName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null);
        simpleCollectionName = CommonFunction.createSimpleCollection(CommonData.dim, null);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(simpleCollectionName).build());
    }

    @Test(description = "insert test",groups = {"Smoke"})
    public void insert(){
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(CommonData.numberEntities, CommonData.dim);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder()
                .data(jsonObjects)
                .collectionName(newCollectionName)
                .build());
        Assert.assertEquals(insert.getInsertCnt(),CommonData.numberEntities);
    }

    @Test(description = "insert simple collection test",groups = {"Smoke"})
    public void insertIntoSimpleCollection(){
        List<JsonObject> jsonObjects = CommonFunction.generateSimpleData(CommonData.numberEntities, CommonData.dim);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder()
                .data(jsonObjects)
                .collectionName(simpleCollectionName)
                .build());
        Assert.assertEquals(insert.getInsertCnt(),CommonData.numberEntities);
    }

}

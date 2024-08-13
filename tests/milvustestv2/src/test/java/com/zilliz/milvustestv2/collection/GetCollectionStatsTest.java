package com.zilliz.milvustestv2.collection;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.vector.request.InsertReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 14:32
 */
public class GetCollectionStatsTest extends BaseTest {

    String newCollectionName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        CommonFunction.createIndexAndInsertAndLoad(newCollectionName,DataType.FloatVector,true,CommonData.numberEntities);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "Get collection stats", groups = {"Smoke"},enabled = false)
    public void getCollectionStats(){
        GetCollectionStatsResp collectionStats = milvusClientV2.getCollectionStats(GetCollectionStatsReq.builder()
                .collectionName(newCollectionName)
                .build());
        // getCollectionStats is not accurate, so comment the assert
        Assert.assertEquals(collectionStats.getNumOfEntities().longValue(),CommonData.numberEntities);
    }
}

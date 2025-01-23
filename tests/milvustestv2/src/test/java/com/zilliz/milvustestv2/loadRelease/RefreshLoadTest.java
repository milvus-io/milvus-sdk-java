package com.zilliz.milvustestv2.loadRelease;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.vector.request.InsertReq;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class RefreshLoadTest extends BaseTest {

    String newCollectionName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim,DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        CommonFunction.createVectorIndex(newCollectionName,CommonData.fieldFloatVector, IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "refresh load collection",groups = {"Smoke"})
    public void refreshLoadTest(){
        milvusClientV2.refreshLoad(RefreshLoadReq.builder().collectionName(newCollectionName)
                .build());
        try {
            Thread.sleep(10*1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Boolean loadState = milvusClientV2.getLoadState(GetLoadStateReq.builder().collectionName(newCollectionName).build());
        Assert.assertTrue(loadState);
    }

    @Test(description = "refresh load collection where the collection not loaded",groups ={"L1"})
    public void refreshLoadUnloadedCollection(){
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder()
                .collectionName(newCollectionName).build());
        try {
            milvusClientV2.refreshLoad(RefreshLoadReq.builder().collectionName(newCollectionName)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("collection not loaded"));
        }

    }

}

package com.zilliz.milvustestv2.loadRelease;

import com.google.gson.JsonObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/23 14:59
 */
public class LoadCollectionTest extends BaseTest {
    String newCollectionName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(CommonData.numberEntities, CommonData.dim);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        CommonFunction.createVectorIndex(newCollectionName,CommonData.fieldFloatVector, IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "Load collection",groups = {"Smoke"})
    public void loadCollection(){
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .build());
        SearchResp searchResp = CommonFunction.defaultSearch(newCollectionName);
        Assert.assertEquals(searchResp.getSearchResults().size(), CommonData.topK);
    }


}

package com.zilliz.milvustestv2.index;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/20 16:34
 */
public class DropIndexTest extends BaseTest {
    String newCollectionName;
    @BeforeClass(alwaysRun = true)
    public void providerCollection(){
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0,CommonData.numberEntities, CommonData.dim,DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.HNSW))
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData(){
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "Drop index",groups = {"Smoke"})
    public void dropIndex(){
        milvusClientV2.dropIndex(DropIndexReq.builder()
                .collectionName(newCollectionName)
                .fieldName(CommonData.fieldFloatVector)
                .build());

    }
}

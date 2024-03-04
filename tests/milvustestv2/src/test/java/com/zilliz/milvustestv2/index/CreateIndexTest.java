package com.zilliz.milvustestv2.index;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
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
public class CreateIndexTest extends BaseTest {
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

    @Test(description = "Create vector index",groups = {"Smoke"})
    public void createVectorIndex(){
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
               .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(newCollectionName).build());

    }

    @Test(description = "Create scalar index",groups = {"Smoke"},dependsOnMethods = {"createVectorIndex"})
    public void createScalarIndex(){
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldVarchar)
                .indexType(IndexParam.IndexType.TRIE)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .build());
    }

    @Test(description = "Create scalar index",groups = {"Smoke"},dependsOnMethods = {"createVectorIndex"})
    public void createMultiScalarIndex(){
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        IndexParam indexParam1 = IndexParam.builder()
                .fieldName(CommonData.fieldVarchar)
                .indexType(IndexParam.IndexType.TRIE)
                .build();
        IndexParam indexParam2 = IndexParam.builder()
                .fieldName(CommonData.fieldInt64)
                .indexType(IndexParam.IndexType.STL_SORT)
                .build();
        IndexParam indexParam3 = IndexParam.builder()
                .fieldName(CommonData.fieldInt8)
                .indexType(IndexParam.IndexType.STL_SORT)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Lists.newArrayList(indexParam1,indexParam2,indexParam3))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .build());
    }
}

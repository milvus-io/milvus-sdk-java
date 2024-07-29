package com.zilliz.milvustestv2.index;

import com.google.gson.JsonObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.params.FieldParam;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/20 16:34
 */
@Slf4j
public class CreateIndexTest extends BaseTest {
    String newCollectionName;

    @DataProvider(name = "multiScalar")
    public Object[][] providerScalarIndex() {
        return new Object[][]{
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldInt8).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt16).indextype(IndexParam.IndexType.STL_SORT).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt64).indextype(IndexParam.IndexType.INVERTED).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldVarchar).indextype(IndexParam.IndexType.TRIE).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldInt16).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldInt32).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldInt64).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldVarchar).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt8).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt16).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt32).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt64).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldBool).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldArray).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
        };
    }

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(CommonData.numberEntities * 10, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "Create vector index", groups = {"Smoke"})
    public void createVectorIndex() {
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

    @Test(description = "Create scalar index", groups = {"Smoke"}, dependsOnMethods = {"createVectorIndex"})
    public void createScalarIndex() {
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

    @Test(description = "Create scalar index", groups = {"Smoke"}, dependsOnMethods = {"createVectorIndex"})
    public void createMultiScalarIndex() {
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
        IndexParam indexParam4 = IndexParam.builder()
                .fieldName(CommonData.fieldInt16)
                .indexType(IndexParam.IndexType.BITMAP)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Lists.newArrayList(indexParam1, indexParam2, indexParam3, indexParam4))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .build());
    }

    @Test(description = "Create scalar index", groups = {"Smoke"}, dependsOnMethods = {"createVectorIndex"}, dataProvider = "multiScalar")
    public void createAllBitmapIndex(List<FieldParam> FieldParamList) {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        CommonFunction.dropScalarCommonIndex(newCollectionName, FieldParamList);
        CommonFunction.createScalarCommonIndex(newCollectionName, FieldParamList);
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        CommonFunction.dropScalarCommonIndex(newCollectionName, FieldParamList);
    }
}

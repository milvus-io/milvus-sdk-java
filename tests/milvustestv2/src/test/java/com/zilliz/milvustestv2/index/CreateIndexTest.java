package com.zilliz.milvustestv2.index;

import com.google.gson.JsonObject;
import com.google.common.collect.Lists;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.params.FieldParam;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.*;

import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2024/2/20 16:34
 */
@Slf4j
public class CreateIndexTest extends BaseTest {
    String newCollectionName;

    String dynamicCollectionName;

    int topK=100;

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
                    add(FieldParam.builder().fieldName(CommonData.fieldVarchar).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldBool).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldArray).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
                {new ArrayList<FieldParam>() {{
                    add(FieldParam.builder().fieldName(CommonData.fieldVarchar).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt8).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt16).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldInt32).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldBool).indextype(IndexParam.IndexType.BITMAP).build());
                    add(FieldParam.builder().fieldName(CommonData.fieldArray).indextype(IndexParam.IndexType.BITMAP).build());
                }}
                },
        };
    }



    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0, CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());

        // 创建dynamicCollectionName
        dynamicCollectionName=CommonFunction.createNewCollectionWithDynamic(CommonData.dim,null,DataType.FloatVector);
        List<JsonObject> jsonObjectsDynamic =CommonFunction.generateDefaultDataWithDynamic(0,CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(dynamicCollectionName).data(jsonObjectsDynamic).build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(dynamicCollectionName).build());
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
    @Test(description = "Create vector index with dynamic", groups = {"Smoke"})
    public void createVectorIndexWithDynamic() {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(dynamicCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(dynamicCollectionName).build());

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

    @Test(description = "Create scalar index",
            groups = {"Smoke"},
            dependsOnMethods = {"createVectorIndex"},
            dataProvider = "multiScalar")
    public void createAllBitmapIndex(List<FieldParam> FieldParamList) {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        CommonFunction.dropScalarCommonIndex(newCollectionName, FieldParamList);
        CommonFunction.createScalarCommonIndex(newCollectionName, FieldParamList);
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        CommonFunction.dropScalarCommonIndex(newCollectionName, FieldParamList);
    }

    @DataProvider(name = "jsonPathIndex")
    public Object[][] providerJsonPathIndex() {
        return new Object[][]{
                {"json_index_0", DataType.Double, CommonData.fieldJson, CommonData.fieldJson + "[\"" + CommonData.fieldInt64 + "\"] in [1,2,3]", 3},
                {"json_index_1", DataType.Double, CommonData.fieldJson + "['" + CommonData.fieldFloat + "']", CommonData.fieldJson + "[\"" + CommonData.fieldFloat + "\"] < 5", 5},
                {"json_index_2", DataType.Double, CommonData.fieldJson + "['" + CommonData.fieldArray + "'][0]", CommonData.fieldJson + "['" + CommonData.fieldArray + "'][0] == 1", 1},
                {"json_index_3",DataType.Bool,CommonData.fieldJson + "['" + CommonData.fieldBool + "']", CommonData.fieldJson + "[\"" + CommonData.fieldBool + "\"] == true", topK },
                {"json_index_4",DataType.VarChar,CommonData.fieldJson+"['"+CommonData.fieldVarchar+"']",CommonData.fieldJson+"[\""+CommonData.fieldVarchar+"\"] == \"Str1\"",1},
        };
    }

    @DataProvider(name = "jsonPathIndexDynamic")
    public Object[][] providerJsonPathIndexDynamic() {
        return new Object[][]{
                {"json_index_0", DataType.Double, CommonData.fieldDynamic, CommonData.fieldDynamic + "[\"" + CommonData.fieldInt64 + "\"] in [1,2,3]", 3},
                {"json_index_1", DataType.Double, CommonData.fieldDynamic + "['" + CommonData.fieldFloat + "']", CommonData.fieldDynamic + "[\"" + CommonData.fieldFloat + "\"] < 5", 5},
                {"json_index_2", DataType.Double, CommonData.fieldDynamic + "['" + CommonData.fieldArray + "'][0]", CommonData.fieldDynamic + "[\"" + CommonData.fieldArray + "\"][0] == 1", 1},
                {"json_index_3",DataType.Bool,CommonData.fieldDynamic + "['" + CommonData.fieldBool + "']", CommonData.fieldDynamic + "[\"" + CommonData.fieldBool + "\"] == true", topK },
                {"json_index_4",DataType.VarChar,CommonData.fieldDynamic+"['"+CommonData.fieldVarchar+"']",CommonData.fieldDynamic+"[\""+CommonData.fieldVarchar+"\"] == \"Str1\"",1},
        };
    }
    @Test(description = "Create json path index", groups = {"Smoke"}, dependsOnMethods = {"createVectorIndex"}, dataProvider = "jsonPathIndex")
    public void createJsonPathIndex(String indexName, DataType jsonCastType, String jsonPath, String filter, int except) {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        Map<String, Object> params = new HashMap<>();
        params.put("json_cast_type", jsonCastType);
        params.put("json_path", jsonPath);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldJson)
                .indexType(IndexParam.IndexType.INVERTED)
                .indexName(indexName)
                .extraParams(params)
                .build();
        System.out.println(indexParam.toString());
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        // describe index
        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder().collectionName(newCollectionName).indexName(indexName).build());
        Assert.assertEquals(describeIndexResp.getIndexDescByIndexName(indexName).getIndexType(), IndexParam.IndexType.INVERTED);

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(newCollectionName)
                .build());

        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(newCollectionName)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .filter(filter)
                .data(data)
                .topK(topK)
                .build());
        Assert.assertEquals(search.getSearchResults().get(0).size(), except);
        System.out.println(search.getSearchResults().get(0).get(0).toString());
    }

    @Test(description = "Create json path index with dynamic field", groups = {"Smoke"}, dependsOnMethods = {"createVectorIndexWithDynamic"}, dataProvider = "jsonPathIndexDynamic")
    public void createJsonPathIndexWithDynamic(String indexName, DataType jsonCastType, String jsonPath, String filter, int except) {
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(dynamicCollectionName).build());

        Map<String, Object> params = new HashMap<>();
        params.put("json_cast_type", jsonCastType);
        params.put("json_path", jsonPath);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldDynamic)
                .indexType(IndexParam.IndexType.INVERTED)
                .indexName(indexName)
                .extraParams(params)
                .build();
        System.out.println(indexParam.toString());
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(dynamicCollectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        // describe index
        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder().collectionName(dynamicCollectionName).indexName(indexName).build());
        Assert.assertEquals(describeIndexResp.getIndexDescByIndexName(indexName).getIndexType(), IndexParam.IndexType.INVERTED);

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(dynamicCollectionName)
                .build());

        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(dynamicCollectionName)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .filter(filter)
                .data(data)
                .topK(topK)
                .build());
        Assert.assertEquals(search.getSearchResults().get(0).size(), except);
    }

    @Test(description = "Dynamic collection create json path index with not existed field ",groups = {"L1"})
    public void dynamicCollectionCreateJsonPathIndexWithNotExistedField(){
        String newCollectionWithDynamic = CommonFunction.createNewCollectionWithDynamic(CommonData.dim, null, DataType.FloatVector);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionWithDynamic)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        // create json index
        Map<String, Object> params = new HashMap<>();
        params.put("json_cast_type", DataType.Double);
        params.put("json_path", CommonData.fieldDynamicNotExist);
        IndexParam indexParam2 = IndexParam.builder()
                .fieldName(CommonData.fieldDynamicNotExist)
                .indexType(IndexParam.IndexType.INVERTED)
                .indexName("not_existed_index")
                .extraParams(params)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollectionWithDynamic)
                .indexParams(Collections.singletonList(indexParam2))
                .build());

        // describe index
        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder().collectionName(newCollectionWithDynamic).indexName("not_existed_index").build());
        System.out.println("describeIndexResp: "+describeIndexResp.toString());
    }

    @Test(description = "Create json path index with not existed field ",groups = {"L1"})
    public void createJsonPathIndexWithNotExistedField(){
        String newCollection = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(CommonFunction.provideExtraParam(IndexParam.IndexType.AUTOINDEX))
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(newCollection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        // create json index
        Map<String, Object> params = new HashMap<>();
        params.put("json_cast_type", DataType.Double);
        params.put("json_path", CommonData.fieldDynamicNotExist);
        IndexParam indexParam2 = IndexParam.builder()
                .fieldName(CommonData.fieldDynamicNotExist)
                .indexType(IndexParam.IndexType.INVERTED)
                .indexName("not_existed_index")
                .extraParams(params)
                .build();
        try {
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(newCollection)
                    .indexParams(Collections.singletonList(indexParam2))
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot create index on non-exist field"));
        }

    }

    @Test(description = "Create json path index repeatedly",groups = {"L1"},dependsOnMethods = {"createVectorIndex","createJsonPathIndex"})
    public void createJsonPathIndexRepeatedly(){
        milvusClientV2.releaseCollection(ReleaseCollectionReq.builder().collectionName(newCollectionName).build());
        Map<String, Object> params = new HashMap<>();
        params.put("json_cast_type", DataType.Double);
        params.put("json_path", CommonData.fieldJson);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldJson)
                .indexType(IndexParam.IndexType.INVERTED)
                .indexName("indexName_repeat")
                .extraParams(params)
                .build();
        try {
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(newCollectionName)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("creating multiple indexes on same field is not supported"));
        }
    }

}

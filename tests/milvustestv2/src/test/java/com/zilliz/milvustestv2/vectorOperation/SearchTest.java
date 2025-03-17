package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.params.FieldParam;
import com.zilliz.milvustestv2.utils.DataProviderUtils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.response.SearchResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:03
 */
@Slf4j
public class SearchTest extends BaseTest {
    int topK = 10;
    private MilvusServiceClient milvusServiceClient;
    String newCollectionName;
    String nullableDefaultCollectionName;

    @DataProvider(name = "filterAndExcept")
    public Object[][] providerData() {
        return new Object[][]{
                {CommonData.fieldInt64 + " != 10 ", topK},
                {CommonData.fieldInt64 + " < 10 ", topK},
                {CommonData.fieldVarchar + " like \"%0\" ", topK},
                {CommonData.fieldInt64 + " <= 10 ", topK},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", topK},
                {CommonData.fieldInt64 + " > 100 ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + "== true", topK / 2},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + ">5", 4},
                {CommonData.fieldVarchar + " > \"Str5\" ", topK},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " like \"Str%\" ", topK},
                {CommonData.fieldVarchar + " like \"Str1\" ", 1},
                {CommonData.fieldInt8 + " > 129 ", 0},
                {"array_contains(" + CommonData.fieldArray + ", 1)", 2},
                {"array_contains_all(" + CommonData.fieldArray + ", [1, 2])", 2},
                {"array_length(" + CommonData.fieldArray + ") == 3", 10},

        };
    }

    @DataProvider(name = "VectorTypeList")
    public Object[][] providerVectorType() {
        return new Object[][]{
                {CommonData.defaultFloatVectorCollection, DataType.FloatVector},
//                {CommonData.defaultBinaryVectorCollection,DataType.BinaryVector},
                {CommonData.defaultFloat16VectorCollection, DataType.Float16Vector},
                {CommonData.defaultBFloat16VectorCollection, DataType.BFloat16Vector},
                {CommonData.defaultSparseFloatVectorCollection, DataType.SparseFloatVector},
        };
    }

    @DataProvider(name = "VectorTypeWithFilter")
    public Object[][] providerVectorTypeWithFilter() {
        Object[][] vectorType = new Object[][]{
                {DataType.FloatVector},
                {DataType.BinaryVector},
                {DataType.Float16Vector},
                {DataType.BFloat16Vector},
                {DataType.SparseFloatVector}
        };
        Object[][] filter = new Object[][]{
                {CommonData.fieldVarchar + " like \"%0\" ", topK},
                {CommonData.fieldInt64 + " < 10 ", topK},
                {CommonData.fieldInt64 + " != 10 ", topK},
                {CommonData.fieldInt64 + " <= 10 ", topK},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", topK},
                {CommonData.fieldInt64 + " > 100 ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + "== true", topK / 2},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", topK},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + ">5", 4},
                {CommonData.fieldVarchar + " > \"Str5\" ", topK},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " like \"Str%\" ", topK},
                {CommonData.fieldVarchar + " like \"Str1\" ", 1},
                {CommonData.fieldInt8 + " > 129 ", 0},

        };
        Object[][] objects = DataProviderUtils.generateDataSets(vectorType, filter);
        return objects;
    }

    @DataProvider(name = "searchPartition")
    private Object[][] providePartitionSearchParams() {
        return new Object[][]{
                {Lists.newArrayList(CommonData.partitionNameA), "0 < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities, topK},
                {Lists.newArrayList(CommonData.partitionNameA), CommonData.numberEntities + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 2, 0},
                {Lists.newArrayList(CommonData.partitionNameB), CommonData.numberEntities + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 2, topK},
                {Lists.newArrayList(CommonData.partitionNameB), CommonData.numberEntities * 2 + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 3, 0},
                {Lists.newArrayList(CommonData.partitionNameC), CommonData.numberEntities * 2 + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 3, topK},
                {Lists.newArrayList(CommonData.partitionNameC), CommonData.numberEntities * 3 + " < " + CommonData.fieldInt64 + " < " + CommonData.numberEntities * 4, 0}
        };
    }

    @DataProvider(name = "searchNullableField")
    private Object[][] provideNullableFieldSearchParams() {
        return new Object[][]{
                {CommonData.fieldInt32 + " == 1 ", topK},
                {CommonData.fieldDouble + " > 1 ", topK},
                {CommonData.fieldVarchar + " == \"1.0\" ", topK},
                {CommonData.fieldFloat + " == 1.0 ", topK},
                {"fieldJson[\"" + CommonData.fieldVarchar + "\"] in [\"Str1\", \"Str3\"]", 0},
                {"ARRAY_CONTAINS(" + CommonData.fieldArray + ", 1)", 1},
        };
    }

    @DataProvider(name = "collectionNameList")
    public Object[][] providerCollectionName() {
        return new Object[][]{
                {newCollectionName},
                {nullableDefaultCollectionName},
        };
    }

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        newCollectionName = CommonFunction.createNewCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> jsonObjects = CommonFunction.generateDefaultData(0, CommonData.numberEntities * 10, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(newCollectionName).data(jsonObjects).build());
        nullableDefaultCollectionName = CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, null, DataType.FloatVector);
        // insert data
        List<JsonObject> jsonNullableObjects = CommonFunction.generateSimpleNullData(0, CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(nullableDefaultCollectionName).data(jsonNullableObjects).build());
        // create partition
        CommonFunction.createPartition(nullableDefaultCollectionName, CommonData.partitionNameA);
        List<JsonObject> jsonObjectsA = CommonFunction.generateSimpleNullData(0, CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(nullableDefaultCollectionName).partitionName(CommonData.partitionNameA).data(jsonObjectsA).build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(newCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(nullableDefaultCollectionName).build());
    }

    @Test(description = "Create vector and scalar index", groups = {"Smoke"}, dataProvider = "collectionNameList")
    public void createVectorAndScalarIndex(String newCollectionName) {
        // Build Vector index
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

        // Build Scalar Index
        List<FieldParam> FieldParamList = new ArrayList<FieldParam>() {{
            add(FieldParam.builder().fieldName(CommonData.fieldVarchar).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt8).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt16).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt32).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldInt64).indextype(IndexParam.IndexType.STL_SORT).build());
            add(FieldParam.builder().fieldName(CommonData.fieldBool).indextype(IndexParam.IndexType.BITMAP).build());
            add(FieldParam.builder().fieldName(CommonData.fieldArray).indextype(IndexParam.IndexType.BITMAP).build());
        }};
        CommonFunction.createScalarCommonIndex(newCollectionName, FieldParamList);
        log.info("Create Scalar index done{}, scalar index:{}", newCollectionName, FieldParamList);
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(newCollectionName).build());
    }

    @Test(description = "search float vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchFloatVectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search binary vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchBinaryVectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.BinaryVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultBinaryVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search bf16 vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchBF16VectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.BFloat16Vector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultBFloat16VectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search float16 vector collection", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchFloat16VectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.Float16Vector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloat16VectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search Sparse vector collection", groups = {"Smoke"})
    public void searchSparseVectorCollection() {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.SparseFloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultSparseFloatVectorCollection)
                .filter("fieldVarchar like\"%0\"")
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
//        Assert.assertEquals(search.getSearchResults().get(0).size(), topK);
    }

    @Test(description = "default search output params return id and distance", groups = {"Smoke"})
    public void searchWithDefaultOutput() {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(CommonData.fieldInt64 + " < 10 ")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), topK);
        Assert.assertEquals(search.getSearchResults().get(0).get(0).getEntity().keySet().size(), 0);
    }

    @Test(description = "search in partition", groups = {"Smoke"}, dataProvider = "searchPartition")
    public void searchInPartition(List<String> partitionName, String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .partitionNames(partitionName)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search by alias", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void searchByAlias(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.alias)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search group by field name", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchByGroupByField(String collectionName, DataType vectorType) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .groupByFieldName(CommonData.fieldInt8)
                .data(data)
                .topK(1000)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertEquals(search.getSearchResults().get(0).size(), 127);
        }
    }

    @Test(description = "search scalar index collection", groups = {"Smoke"}, dependsOnMethods = {"createVectorAndScalarIndex"}, dataProvider = "filterAndExcept")
    public void searchScalarIndexCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchReq searchParams = SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build();
        System.out.println(searchParams);
        SearchResp search = milvusClientV2.search(searchParams);
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search collection with nullable field", groups = {"Smoke"}, dependsOnMethods = {"createVectorAndScalarIndex"}, dataProvider = "searchNullableField")
    public void searchNullableCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);
        SearchReq searchParams = SearchReq.builder()
                .collectionName(nullableDefaultCollectionName)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .topK(topK)
                .build();
        System.out.println(searchParams);
        SearchResp search = milvusClientV2.search(searchParams);
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), expect);
    }

    @Test(description = "search by group size", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchByGroupSize(String collectionName, DataType vectorType) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .groupByFieldName(CommonData.fieldInt8)
                .groupSize(CommonData.groupSize)
                .data(data)
                .topK(1000)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertTrue(search.getSearchResults().get(0).size() > 127);
        }
    }

    @Test(description = "search by group size and topK", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchByGroupSizeAndTopK(String collectionName, DataType vectorType) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .groupByFieldName(CommonData.fieldInt8)
                .groupSize(CommonData.groupSize)
                .data(data)
                .topK(10)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertTrue(search.getSearchResults().get(0).size() >= 10);
        }
    }

    @Test(description = "search by group size and topK and strict", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchByGroupSizeAndTopKAndStrict(String collectionName, DataType vectorType) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .groupByFieldName(CommonData.fieldInt8)
                .groupSize(CommonData.groupSize)
                .data(data)
                .strictGroupSize(true)
                .topK(10)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertEquals(search.getSearchResults().get(0).size(), 10 * CommonData.groupSize);
        }
    }

    @Test(description = "search enable recall calculation", groups = {"Cloud","L1"}, dataProvider = "VectorTypeList")
    public void searchEnableRecallCalculation(String collectionName, DataType vectorType) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        Map<String, Object> params = new HashMap<>();
        params.put("level", 1);
        params.put("enable_recall_calculation", true);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .topK(10)
                .searchParams(params)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        // 云上实例才有recall
       /* if (vectorType != DataType.SparseFloatVector) {
            Assert.assertTrue(search.getRecalls().get(0) > 0);
        }*/
    }

    @Test(description = "search with expression template", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchWithExpressionTemplate(String collectionName, DataType vectorType) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        Map<String, Map<String, Object>> expressionTemplateValues = new HashMap<>();
        List<Object> list = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        Map<String, Object> params = new HashMap<>();
        params.put("list", list);
        params.put("list2", Lists.newArrayList(11,12,13,14,15,16,17,18,19,20));
        expressionTemplateValues.put(CommonData.fieldInt64 + " in {list}", params);
        expressionTemplateValues.put(CommonData.fieldInt64 + " in {list2}", params);
        expressionTemplateValues.forEach((key, value) -> {
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .filter(key)
                    .filterTemplateValues(value)
                    .data(data)
                    .topK(100)
                    .build());
            Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
            if (vectorType != DataType.SparseFloatVector) {
                Assert.assertEquals(search.getSearchResults().get(0).size(), 10);
            }
        });
    }

    @Test(description = "search use hints", groups = {"Smoke"}, dataProvider = "VectorTypeList")
    public void searchWithHints(String collectionName, DataType vectorType){
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, vectorType);
        Map<String,Object> params=new HashMap<>();
        params.put("hints","iterative_filter");
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .searchParams(params)
                .topK(10)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        if (vectorType != DataType.SparseFloatVector) {
            Assert.assertEquals(search.getSearchResults().get(0).size(), 10 );
        }
    }
}

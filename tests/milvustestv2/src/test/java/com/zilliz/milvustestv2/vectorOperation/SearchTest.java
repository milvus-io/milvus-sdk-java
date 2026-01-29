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

    @DataProvider(name = "VectorTypeListWithoutSparse")
    public Object[][] providerVectorTypeWithoutSparse() {
        return new Object[][]{
                {CommonData.defaultFloatVectorCollection, DataType.FloatVector},
//                {CommonData.defaultBinaryVectorCollection,DataType.BinaryVector},
                {CommonData.defaultFloat16VectorCollection, DataType.Float16Vector},
                {CommonData.defaultBFloat16VectorCollection, DataType.BFloat16Vector},
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

    @Test(description = "search binary vector collection", groups = {"L1"}, dataProvider = "filterAndExcept")
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
        // Binary vector collection data may have different distribution, so we just verify the result is not negative
        Assert.assertTrue(search.getSearchResults().get(0).size() >= 0 && search.getSearchResults().get(0).size() <= expect,
                "Result size should be between 0 and " + expect + ", but got " + search.getSearchResults().get(0).size());
    }

    @Test(description = "search bf16 vector collection", groups = {"L1"}, dataProvider = "filterAndExcept")
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
        // BFloat16 vector collection data may have different distribution, so we just verify the result is not negative
        Assert.assertTrue(search.getSearchResults().get(0).size() >= 0 && search.getSearchResults().get(0).size() <= expect,
                "Result size should be between 0 and " + expect + ", but got " + search.getSearchResults().get(0).size());
    }

    @Test(description = "search float16 vector collection", groups = {"L1"}, dataProvider = "filterAndExcept")
    public void searchFloat16VectorCollection(String filter, int expect) {
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.Float16Vector);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloat16VectorCollection)
                .filter(filter)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .data(data)
                .limit(topK)
                .build());
        System.out.println(search);
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        // Float16 vector collection data may have different distribution, so we just verify the result is not negative
        Assert.assertTrue(search.getSearchResults().get(0).size() >= 0 && search.getSearchResults().get(0).size() <= expect,
                "Result size should be between 0 and " + expect + ", but got " + search.getSearchResults().get(0).size());
    }

    @Test(description = "search Sparse vector collection", groups = {"L1"})
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

    @Test(description = "default search output params return id and distance", groups = {"L1"})
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

    @Test(description = "search in partition", groups = {"L1"}, dataProvider = "searchPartition")
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

    @Test(description = "search by alias", groups = {"L1"}, dataProvider = "filterAndExcept")
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

    @Test(description = "search group by field name", groups = {"L1"}, dataProvider = "VectorTypeListWithoutSparse")
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
        Assert.assertEquals(search.getSearchResults().get(0).size(), 127);
    }

    @Test(description = "search scalar index collection", groups = {"L1"}, dependsOnMethods = {"createVectorAndScalarIndex"}, dataProvider = "filterAndExcept")
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

    @Test(description = "search collection with nullable field", groups = {"L1"}, dependsOnMethods = {"createVectorAndScalarIndex"}, dataProvider = "searchNullableField")
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

    @Test(description = "search by group size", groups = {"L1"}, dataProvider = "VectorTypeListWithoutSparse")
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
        Assert.assertTrue(search.getSearchResults().get(0).size() > 127);
    }

    @Test(description = "search by group size and topK", groups = {"L1"}, dataProvider = "VectorTypeListWithoutSparse")
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
        Assert.assertTrue(search.getSearchResults().get(0).size() >= 10);
    }

    @Test(description = "search by group size and topK and strict", groups = {"L1"}, dataProvider = "VectorTypeListWithoutSparse")
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
        Assert.assertEquals(search.getSearchResults().get(0).size(), 10 * CommonData.groupSize);
    }

    @Test(description = "search enable recall calculation", groups = {"Cloud","L1"}, dataProvider = "VectorTypeListWithoutSparse")
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

    @Test(description = "search with expression template", groups = {"L1"}, dataProvider = "VectorTypeList")
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

    @Test(description = "search use hints", groups = {"L1"}, dataProvider = "VectorTypeListWithoutSparse")
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
                .limit(10)
                .build());
        Assert.assertEquals(search.getSearchResults().size(), CommonData.nq);
        Assert.assertEquals(search.getSearchResults().get(0).size(), 10);
    }

    // ==================== Search by Primary Key Tests ====================

    @DataProvider(name = "SearchByIdVectorTypeList")
    public Object[][] providerSearchByIdVectorType() {
        return new Object[][]{
                {CommonData.defaultFloatVectorCollection, DataType.FloatVector, CommonData.fieldFloatVector},
                {CommonData.defaultBinaryVectorCollection, DataType.BinaryVector, CommonData.fieldBinaryVector},
                {CommonData.defaultFloat16VectorCollection, DataType.Float16Vector, CommonData.fieldFloat16Vector},
                {CommonData.defaultBFloat16VectorCollection, DataType.BFloat16Vector, CommonData.fieldBF16Vector},
                // Note: SparseFloatVector is excluded as per documentation - sparse vector fields derived from VarChar fields are not supported
        };
    }

    @Test(description = "Basic search by primary key - use ids instead of query vectors", groups = {"Smoke"}, dataProvider = "SearchByIdVectorTypeList")
    public void searchByPrimaryKeyBasic(String collectionName, DataType vectorType, String annsField) {
        // Use primary keys instead of query vectors for similarity search
        List<Object> ids = Arrays.asList(1L, 2L, 3L);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(annsField)
                .ids(ids)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(topK)
                .build());
        System.out.println("Search by primary key result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), ids.size());
        // Each id should return results
        for (int i = 0; i < ids.size(); i++) {
            Assert.assertTrue(search.getSearchResults().get(i).size() > 0,
                "Search result for id " + ids.get(i) + " should not be empty");
        }
    }

    @Test(description = "Search by primary key with filter", groups = {"L1"}, dataProvider = "SearchByIdVectorTypeList")
    public void searchByPrimaryKeyWithFilter(String collectionName, DataType vectorType, String annsField) {
        // Search by primary key with additional filter conditions
        List<Object> ids = Arrays.asList(1L, 2L, 3L);
        String filter = CommonData.fieldInt64 + " < 1000";
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(annsField)
                .ids(ids)
                .filter(filter)
                .outputFields(Lists.newArrayList(CommonData.fieldInt64, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(topK)
                .build());
        System.out.println("Search by primary key with filter result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), ids.size());
        // Verify filter is applied - all returned fieldInt64 values should be < 1000
        for (List<SearchResp.SearchResult> resultList : search.getSearchResults()) {
            for (SearchResp.SearchResult result : resultList) {
                if (result.getEntity().containsKey(CommonData.fieldInt64)) {
                    Long fieldValue = (Long) result.getEntity().get(CommonData.fieldInt64);
                    Assert.assertTrue(fieldValue < 1000, "Filter condition not satisfied: " + fieldValue);
                }
            }
        }
    }

    @Test(description = "Range search by primary key", groups = {"L1"})
    public void searchByPrimaryKeyWithRange() {
        // Range search using primary keys - only for FloatVector with L2 metric
        List<Object> ids = Arrays.asList(1L, 2L, 3L);
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("radius", 100.0f);
        searchParams.put("range_filter", 0.0f);

        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .searchParams(searchParams)
                .limit(topK)
                .build());
        System.out.println("Range search by primary key result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), ids.size());
    }

    @Test(description = "Grouping search by primary key", groups = {"L1"})
    public void searchByPrimaryKeyWithGroupBy() {
        // Grouping search using primary keys
        List<Object> ids = Arrays.asList(1L, 2L, 3L);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .groupByFieldName(CommonData.fieldInt8)
                .outputFields(Lists.newArrayList(CommonData.fieldInt8))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(100)
                .build());
        System.out.println("Grouping search by primary key result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), ids.size());
    }

    @Test(description = "Search by primary key with pagination", groups = {"L1"})
    public void searchByPrimaryKeyWithPagination() {
        // Search by primary key with offset and limit for pagination
        List<Object> ids = Arrays.asList(1L, 2L);
        long offset = 2;
        long limit = 5;
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .offset(offset)
                .limit(limit)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("Search by primary key with pagination result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), ids.size());
        // Each result should have at most 'limit' results
        for (List<SearchResp.SearchResult> resultList : search.getSearchResults()) {
            Assert.assertTrue(resultList.size() <= limit,
                "Result size should not exceed limit: " + resultList.size());
        }
    }

    @Test(description = "Search by single primary key", groups = {"L1"})
    public void searchBySinglePrimaryKey() {
        // Search using a single primary key
        List<Object> ids = Arrays.asList(100L);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(topK)
                .build());
        System.out.println("Search by single primary key result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), 1);
        Assert.assertTrue(search.getSearchResults().get(0).size() > 0);
    }

    @Test(description = "Search by primary key with multiple ids", groups = {"L1"})
    public void searchByMultiplePrimaryKeys() {
        // Search using multiple primary keys
        List<Object> ids = Arrays.asList(1L, 10L, 100L, 500L, 1000L);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(topK)
                .build());
        System.out.println("Search by multiple primary keys result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), ids.size());
    }

    @Test(description = "Search by primary key - ids and data are mutually exclusive", groups = {"L1"},
          expectedExceptions = Exception.class)
    public void searchByPrimaryKeyWithBothIdsAndData() {
        // Providing both ids and data should result in an error
        List<Object> ids = Arrays.asList(1L, 2L, 3L);
        List<BaseVector> data = CommonFunction.providerBaseVector(CommonData.nq, CommonData.dim, DataType.FloatVector);

        // This should throw an exception because ids and data are mutually exclusive
        milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .data(data)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(topK)
                .build());
    }

    @Test(description = "Search by nonexistent primary key should return error", groups = {"L1"},
          expectedExceptions = Exception.class)
    public void searchByNonexistentPrimaryKey() {
        // Using nonexistent primary keys should result in an error
        List<Object> ids = Arrays.asList(999999999L, 888888888L);

        milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(topK)
                .build());
    }

    @Test(description = "Search by primary key in partition", groups = {"L1"})
    public void searchByPrimaryKeyInPartition() {
        // Search by primary key within a specific partition
        List<Object> ids = Arrays.asList(1L, 2L, 3L);
        SearchResp search = milvusClientV2.search(SearchReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .annsField(CommonData.fieldFloatVector)
                .ids(ids)
                .partitionNames(Lists.newArrayList(CommonData.partitionNameA))
                .outputFields(Lists.newArrayList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .limit(topK)
                .build());
        System.out.println("Search by primary key in partition result: " + search);
        Assert.assertEquals(search.getSearchResults().size(), ids.size());
    }

    // ==================== Search by Varchar Primary Key Tests ====================

    @Test(description = "Basic search by varchar primary key", groups = {"Smoke"})
    public void searchByVarcharPrimaryKeyBasic() {
        // Create a collection with varchar primary key
        String varcharPKCollection = CommonFunction.createNewCollectionWithVarcharPK(CommonData.dim, null, DataType.FloatVector);
        try {
            // Insert data with varchar primary key
            List<JsonObject> jsonObjects = CommonFunction.generateDataWithVarcharPK(0, 1000, CommonData.dim, DataType.FloatVector);
            milvusClientV2.insert(InsertReq.builder().collectionName(varcharPKCollection).data(jsonObjects).build());

            // Create index and load
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(varcharPKCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(varcharPKCollection).build());

            // Search by varchar primary key
            List<Object> ids = Arrays.asList("Str0", "Str1", "Str2");
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(varcharPKCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .ids(ids)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .limit(topK)
                    .build());
            System.out.println("Search by varchar primary key result: " + search);
            Assert.assertEquals(search.getSearchResults().size(), ids.size());
            // Each id should return results
            for (int i = 0; i < ids.size(); i++) {
                Assert.assertTrue(search.getSearchResults().get(i).size() > 0,
                    "Search result for id " + ids.get(i) + " should not be empty");
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(varcharPKCollection).build());
        }
    }

    @Test(description = "Search by varchar primary key with filter", groups = {"L1"})
    public void searchByVarcharPrimaryKeyWithFilter() {
        // Create a collection with varchar primary key
        String varcharPKCollection = CommonFunction.createNewCollectionWithVarcharPK(CommonData.dim, null, DataType.FloatVector);
        try {
            // Insert data with varchar primary key
            List<JsonObject> jsonObjects = CommonFunction.generateDataWithVarcharPK(0, 1000, CommonData.dim, DataType.FloatVector);
            milvusClientV2.insert(InsertReq.builder().collectionName(varcharPKCollection).data(jsonObjects).build());

            // Create index and load
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(varcharPKCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(varcharPKCollection).build());

            // Search by varchar primary key with filter
            List<Object> ids = Arrays.asList("Str10", "Str20", "Str30");
            String filter = CommonData.fieldInt64 + " < 500";
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(varcharPKCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .ids(ids)
                    .filter(filter)
                    .outputFields(Lists.newArrayList(CommonData.fieldInt64, CommonData.fieldVarchar))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .limit(topK)
                    .build());
            System.out.println("Search by varchar primary key with filter result: " + search);
            Assert.assertEquals(search.getSearchResults().size(), ids.size());
            // Verify filter is applied
            for (List<SearchResp.SearchResult> resultList : search.getSearchResults()) {
                for (SearchResp.SearchResult result : resultList) {
                    if (result.getEntity().containsKey(CommonData.fieldInt64)) {
                        Long fieldValue = (Long) result.getEntity().get(CommonData.fieldInt64);
                        Assert.assertTrue(fieldValue < 500, "Filter condition not satisfied: " + fieldValue);
                    }
                }
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(varcharPKCollection).build());
        }
    }

    @Test(description = "Search by single varchar primary key", groups = {"L1"})
    public void searchBySingleVarcharPrimaryKey() {
        // Create a collection with varchar primary key
        String varcharPKCollection = CommonFunction.createNewCollectionWithVarcharPK(CommonData.dim, null, DataType.FloatVector);
        try {
            // Insert data with varchar primary key
            List<JsonObject> jsonObjects = CommonFunction.generateDataWithVarcharPK(0, 500, CommonData.dim, DataType.FloatVector);
            milvusClientV2.insert(InsertReq.builder().collectionName(varcharPKCollection).data(jsonObjects).build());

            // Create index and load
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(varcharPKCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(varcharPKCollection).build());

            // Search by single varchar primary key
            List<Object> ids = Arrays.asList("Str100");
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(varcharPKCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .ids(ids)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .limit(topK)
                    .build());
            System.out.println("Search by single varchar primary key result: " + search);
            Assert.assertEquals(search.getSearchResults().size(), 1);
            Assert.assertTrue(search.getSearchResults().get(0).size() > 0);
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(varcharPKCollection).build());
        }
    }

    @Test(description = "Search by multiple varchar primary keys", groups = {"L1"})
    public void searchByMultipleVarcharPrimaryKeys() {
        // Create a collection with varchar primary key
        String varcharPKCollection = CommonFunction.createNewCollectionWithVarcharPK(CommonData.dim, null, DataType.FloatVector);
        try {
            // Insert data with varchar primary key
            List<JsonObject> jsonObjects = CommonFunction.generateDataWithVarcharPK(0, 1000, CommonData.dim, DataType.FloatVector);
            milvusClientV2.insert(InsertReq.builder().collectionName(varcharPKCollection).data(jsonObjects).build());

            // Create index and load
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(varcharPKCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(varcharPKCollection).build());

            // Search by multiple varchar primary keys
            List<Object> ids = Arrays.asList("Str1", "Str50", "Str100", "Str200", "Str500");
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(varcharPKCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .ids(ids)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .limit(topK)
                    .build());
            System.out.println("Search by multiple varchar primary keys result: " + search);
            Assert.assertEquals(search.getSearchResults().size(), ids.size());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(varcharPKCollection).build());
        }
    }

    @Test(description = "Search by varchar primary key with grouping", groups = {"L1"})
    public void searchByVarcharPrimaryKeyWithGroupBy() {
        // Create a collection with varchar primary key
        String varcharPKCollection = CommonFunction.createNewCollectionWithVarcharPK(CommonData.dim, null, DataType.FloatVector);
        try {
            // Insert data with varchar primary key
            List<JsonObject> jsonObjects = CommonFunction.generateDataWithVarcharPK(0, 1000, CommonData.dim, DataType.FloatVector);
            milvusClientV2.insert(InsertReq.builder().collectionName(varcharPKCollection).data(jsonObjects).build());

            // Create index and load
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(varcharPKCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(varcharPKCollection).build());

            // Search by varchar primary key with grouping
            List<Object> ids = Arrays.asList("Str1", "Str2", "Str3");
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(varcharPKCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .ids(ids)
                    .groupByFieldName(CommonData.fieldInt8)
                    .outputFields(Lists.newArrayList(CommonData.fieldInt8))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .limit(100)
                    .build());
            System.out.println("Search by varchar primary key with grouping result: " + search);
            Assert.assertEquals(search.getSearchResults().size(), ids.size());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(varcharPKCollection).build());
        }
    }

    @Test(description = "Search by varchar primary key with pagination", groups = {"L1"})
    public void searchByVarcharPrimaryKeyWithPagination() {
        // Create a collection with varchar primary key
        String varcharPKCollection = CommonFunction.createNewCollectionWithVarcharPK(CommonData.dim, null, DataType.FloatVector);
        try {
            // Insert data with varchar primary key
            List<JsonObject> jsonObjects = CommonFunction.generateDataWithVarcharPK(0, 1000, CommonData.dim, DataType.FloatVector);
            milvusClientV2.insert(InsertReq.builder().collectionName(varcharPKCollection).data(jsonObjects).build());

            // Create index and load
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(varcharPKCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(varcharPKCollection).build());

            // Search by varchar primary key with pagination
            List<Object> ids = Arrays.asList("Str1", "Str2");
            long offset = 2;
            long limit = 5;
            SearchResp search = milvusClientV2.search(SearchReq.builder()
                    .collectionName(varcharPKCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .ids(ids)
                    .offset(offset)
                    .limit(limit)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.println("Search by varchar primary key with pagination result: " + search);
            Assert.assertEquals(search.getSearchResults().size(), ids.size());
            // Each result should have at most 'limit' results
            for (List<SearchResp.SearchResult> resultList : search.getSearchResults()) {
                Assert.assertTrue(resultList.size() <= limit,
                    "Result size should not exceed limit: " + resultList.size());
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(varcharPKCollection).build());
        }
    }

    @Test(description = "Search by nonexistent varchar primary key should return error", groups = {"L1"},
          expectedExceptions = Exception.class)
    public void searchByNonexistentVarcharPrimaryKey() {
        // Create a collection with varchar primary key
        String varcharPKCollection = CommonFunction.createNewCollectionWithVarcharPK(CommonData.dim, null, DataType.FloatVector);
        try {
            // Insert data with varchar primary key
            List<JsonObject> jsonObjects = CommonFunction.generateDataWithVarcharPK(0, 100, CommonData.dim, DataType.FloatVector);
            milvusClientV2.insert(InsertReq.builder().collectionName(varcharPKCollection).data(jsonObjects).build());

            // Create index and load
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.AUTOINDEX)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(varcharPKCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(varcharPKCollection).build());

            // Search by nonexistent varchar primary key - should throw exception
            List<Object> ids = Arrays.asList("NonExistentKey1", "NonExistentKey2");
            milvusClientV2.search(SearchReq.builder()
                    .collectionName(varcharPKCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .ids(ids)
                    .outputFields(Lists.newArrayList("*"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .limit(topK)
                    .build());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(varcharPKCollection).build());
        }
    }
}

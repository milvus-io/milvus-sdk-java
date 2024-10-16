package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.params.FieldParam;
import com.zilliz.milvustestv2.utils.DataProviderUtils;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.GetCollectionStatsReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:03
 */
public class QueryTest extends BaseTest {

    String nullableDefaultCollectionName;
    
    @DataProvider(name = "filterAndExcept")
    public Object[][] providerData() {
        return new Object[][]{
                {CommonData.fieldInt64 + " < 10 ", 10},
                {CommonData.fieldInt64 + " != 10 ", CommonData.numberEntities*3 - 1},
                {CommonData.fieldInt64 + " <= 10 ", 11},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", CommonData.numberEntities*3 - 10},
                {CommonData.fieldInt64 + " > 100 ", CommonData.numberEntities*3 - 101},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + " == true", 5},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", CommonData.numberEntities*3 - 3},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + " >5 ", 4},
                {CommonData.fieldVarchar + " > \"0\" ", CommonData.numberEntities*3},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " like \"Str%\" ", CommonData.numberEntities*3},
                {CommonData.fieldVarchar + " like \"Str1\" ", 1},
                {CommonData.fieldInt8 + " > 129 ", 0},
        };
    }

    @DataProvider(name = "queryPartition")
    private Object[][] providePartitionQueryParams() {
        return new Object[][]{
                {Lists.newArrayList(CommonData.partitionNameA), "0 <= " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities, CommonData.numberEntities},
                {Lists.newArrayList(CommonData.partitionNameA), CommonData.numberEntities + " < " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 2, 0},
                {Lists.newArrayList(CommonData.partitionNameB), CommonData.numberEntities + " <= " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 2, CommonData.numberEntities},
                {Lists.newArrayList(CommonData.partitionNameB), CommonData.numberEntities * 2 + " < " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 3, 0},
                {Lists.newArrayList(CommonData.partitionNameC), CommonData.numberEntities * 2 + " <= " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 3, CommonData.numberEntities},
                {Lists.newArrayList(CommonData.partitionNameC), CommonData.numberEntities * 3 + " < " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 4, 0},
                {Lists.newArrayList(CommonData.partitionNameA,CommonData.partitionNameB), "0 <= " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 2, CommonData.numberEntities * 2},
                {Lists.newArrayList(CommonData.partitionNameA,CommonData.partitionNameB,CommonData.partitionNameC),"0 <= " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 3, CommonData.numberEntities * 3},
        };
    }

    @DataProvider(name="DiffCollectionWithFilter")
    public Object[][] providerDiffCollectionWithFilter(){
        Object[][] vectorType=new Object[][]{
                {CommonData.defaultBFloat16VectorCollection},
                {CommonData.defaultBinaryVectorCollection},
                {CommonData.defaultFloat16VectorCollection},
                {CommonData.defaultSparseFloatVectorCollection}

        };
        Object[][] filter=new Object[][]{
                {CommonData.fieldInt64 + " < 10 ", 10},
                {CommonData.fieldInt64 + " != 10 ", CommonData.numberEntities - 1},
                {CommonData.fieldInt64 + " <= 10 ", 11},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", CommonData.numberEntities - 10},
                {CommonData.fieldInt64 + " > 100 ", CommonData.numberEntities - 101},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + " == true", 5},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", CommonData.numberEntities - 3},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + " >5 ", 4},
                {CommonData.fieldVarchar + " > \"0\" ", CommonData.numberEntities},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " like \"Str%\" ", CommonData.numberEntities},
                {CommonData.fieldVarchar + " like \"Str1\" ", 1},
                {CommonData.fieldInt8 + " > 129 ", 0},

        };
        Object[][] objects = DataProviderUtils.generateDataSets(vectorType, filter);
        return objects;
    }

    @BeforeClass(alwaysRun = true)
    public void providerCollection() {
        nullableDefaultCollectionName = CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, null, DataType.FloatVector);
        // insert data
        List<JsonObject> jsonObjects = CommonFunction.generateSimpleNullData(0,CommonData.numberEntities, CommonData.dim,DataType.FloatVector);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(nullableDefaultCollectionName).data(jsonObjects).build());
        CommonFunction.createVectorIndex(nullableDefaultCollectionName,CommonData.fieldFloatVector, IndexParam.IndexType.AUTOINDEX, IndexParam.MetricType.L2);
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
        CommonFunction.createScalarCommonIndex(nullableDefaultCollectionName, FieldParamList);
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(nullableDefaultCollectionName).build());
        // create partition
        CommonFunction.createPartition(nullableDefaultCollectionName,CommonData.partitionNameA);
        List<JsonObject> jsonObjectsA = CommonFunction.generateSimpleNullData(0,CommonData.numberEntities*3, CommonData.dim,DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(nullableDefaultCollectionName).partitionName(CommonData.partitionNameA).data(jsonObjectsA).build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(nullableDefaultCollectionName).build());
    }

    @DataProvider(name = "queryNullableField")
    private Object[][] provideNullableFieldQueryParams() {
        return new Object[][]{
                {CommonData.fieldInt32 + " == 1 ", CommonData.numberEntities*3},
                {CommonData.fieldDouble + " > 1 ", CommonData.numberEntities*3/2 - 1},
                {CommonData.fieldVarchar + " == \"1.0\" ", CommonData.numberEntities*3/2},
                {CommonData.fieldFloat + " == 1.0 ", CommonData.numberEntities*3/2 + 1},
                {"fieldJson[\"" + CommonData.fieldVarchar + "\"] in [\"Str1\", \"Str3\"]", 2},
                {"ARRAY_CONTAINS(" + CommonData.fieldArray + ", 1)", 1},
        };
    }

    @Test(description = "query", groups = {"Smoke"}, dataProvider = "filterAndExcept")
    public void query(String filter, long expect) {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(query.getQueryResults().size(), expect);
    }

    @Test(description = "queryByIds", groups = {"Smoke"})
    public void queryByIds() {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .ids(Lists.newArrayList(1, 2, 3, 4))
                .build());

        Assert.assertEquals(query.getQueryResults().size(), 4);
    }

    @Test(description = "queryByIdsAndFilter", groups = {"Smoke"})
    public void queryByIdsAndFilter() {
        try {
            milvusClientV2.query(QueryReq.builder()
                    .collectionName(CommonData.defaultFloatVectorCollection)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Lists.newArrayList("*"))
                    .ids(Lists.newArrayList(1, 2, 3, 4))
                    .filter(" fieldInt64 in [10] ")
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("filter and ids can't be set at the same time"));
        }

    }


    @Test(description = "queryByAlias", groups = {"Smoke"})
    public void queryByAlias() {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(CommonData.alias)
                .consistencyLevel(ConsistencyLevel.STRONG)
//                .outputFields(Lists.newArrayList("*"))
                .ids(Lists.newArrayList(1, 2, 3, 4))
                .build());
        System.out.println(query);
        Assert.assertEquals(query.getQueryResults().size(), 4);
    }

    @Test(description = "queryInPartition", groups = {"Smoke"}, dataProvider = "queryPartition")
    public void queryInPartition(List<String> partition, String filter, long expect) {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(CommonData.defaultFloatVectorCollection)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .partitionNames(partition)
                .build());
        Assert.assertEquals(query.getQueryResults().size(), expect);
    }

    @Test(description = "query with different collection", groups = {"Smoke"}, dataProvider = "DiffCollectionWithFilter")
    public void queryDiffCollection(String collectionName,String filter, long expect) {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(query.getQueryResults().size(), expect);
    }

    @Test(description = "query with nullable field", groups = {"Smoke"}, dataProvider = "queryNullableField")
    public void queryByNullFilter(String filter, long expect) {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(nullableDefaultCollectionName)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Lists.newArrayList("*"))
                .filter(filter)
                .build());
        Assert.assertEquals(query.getQueryResults().size(), expect);
    }
}

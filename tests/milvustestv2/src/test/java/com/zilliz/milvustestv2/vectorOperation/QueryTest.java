package com.zilliz.milvustestv2.vectorOperation;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.params.FieldParam;
import com.zilliz.milvustestv2.utils.DataProviderUtils;
import com.zilliz.milvustestv2.utils.PropertyFilesUtil;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.GetPersistentSegmentInfoResponse;
import io.milvus.grpc.GetQuerySegmentInfoResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.control.GetQuerySegmentInfoParam;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.internal.reflect.MethodMatcherException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @Author yongpeng.li
 * @Date 2024/2/19 17:03
 */
public class QueryTest extends BaseTest {

    String nullableDefaultCollectionName;
    String samplingCollection;
    String samplingCollectionWithMultiSegment;
    String samplingCollectionWithNullable;
    MilvusServiceClient milvusClientV1;
    long samplingCollectionEntityNum = CommonData.numberEntities * 3;
    long samplingCollectionWithNullableEntityNum = CommonData.numberEntities * 3;
    long samplingCollectionWithMultiSegmentEntityNum = CommonData.numberEntities * 20;

    @DataProvider(name = "filterAndExcept")
    public Object[][] providerData() {
        return new Object[][]{
                {CommonData.fieldInt64 + " >= 0 ", CommonData.numberEntities * 3},
                {CommonData.fieldInt64 + " < 10 ", 10},
                {CommonData.fieldInt64 + " != 10 ", CommonData.numberEntities * 3 - 1},
                {CommonData.fieldInt64 + " <= 10 ", 11},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", CommonData.numberEntities * 3 - 10},
                {CommonData.fieldInt64 + " > 100 ", CommonData.numberEntities * 3 - 101},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + " == true", 5},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", CommonData.numberEntities * 3 - 3},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + " >5 ", 4},
                {CommonData.fieldVarchar + " > \"0\" ", CommonData.numberEntities * 3},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " like \"Str%\" ", CommonData.numberEntities * 3},
                {CommonData.fieldVarchar + " like \"Str1\" ", 1},
                {CommonData.fieldInt8 + " > 129 ", 0},
                {CommonData.fieldInt32 + " > 0 ", CommonData.numberEntities * 3 - 1},
                {CommonData.fieldInt32 + " < 10 ", 10},
                {CommonData.fieldInt32 + " != 10 ", CommonData.numberEntities * 3 - 1},
                {CommonData.fieldInt32 + " <= 10 ", 11},
                {"5<" + CommonData.fieldInt32 + " <= 10 ", 5},
                {CommonData.fieldInt32 + " >= 10 ", CommonData.numberEntities * 3 - 10},
                {CommonData.fieldInt32 + " > 100 ", CommonData.numberEntities * 3 - 101},
                {CommonData.fieldInt32 + " < 10 and " + CommonData.fieldBool + " == true", 5},
                {CommonData.fieldInt32 + " in [1,2,3] ", 3},
                {CommonData.fieldInt32 + " not in [1,2,3] ", CommonData.numberEntities * 3 - 3},
                {CommonData.fieldInt32 + " < 10 and " + CommonData.fieldInt32 + " >5 ", 4},
                {CommonData.fieldFloat + "<= 10", 11},
                {CommonData.fieldArray + "[0] >= 0", CommonData.numberEntities * 3},
                {CommonData.fieldArray + "[0] <= 10", 11},
                {"ARRAY_CONTAINS(" + CommonData.fieldArray + ", 1)", 2},
                {(CommonData.numberEntities * 3 - 10) + " <= " + CommonData.fieldArray + "[0] ", 10},
                {CommonData.fieldArray + "[0] < 10 or " + (CommonData.numberEntities * 3 - 10) + " <= " + CommonData.fieldArray + "[0] ", 20},
                {CommonData.fieldArray + "[0] < 10 ||" + (CommonData.numberEntities * 3 - 10) + " <= " + CommonData.fieldArray + "[0] ", 20},
                {CommonData.fieldArray + "[0] not in [1,2,3]", CommonData.numberEntities * 3 - 3},
                {CommonData.fieldArray + "[0]  in [1,2,3]", 3},
                {CommonData.fieldArray + "[0] != 0", CommonData.numberEntities * 3 - 1},
                {CommonData.fieldArray + "[1] % 100 == 0 ", (CommonData.numberEntities * 3) / 100},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] < 10", 10},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] % 100 == 0", (CommonData.numberEntities * 3) / 100},
                {CommonData.fieldJson + "['" + CommonData.fieldFloat + "'] < 1000 && " + CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] >= 500", 500},
                {CommonData.fieldJson + "['" + CommonData.fieldFloat + "'] < 10", 10},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] in [1,2,3]", 3},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] not in [1,2,3]", CommonData.numberEntities * 3 - 3},
                {"(" + CommonData.fieldJson + "['" + CommonData.fieldFloat + "'] < 10 || " + CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] >= " + (CommonData.numberEntities * 3 - 10) + ")", 20},
        };
    }

    @DataProvider(name = "filterAndExceptWithNullable")
    public Object[][] providerDataWithNullable() {
        return new Object[][]{
                {CommonData.fieldInt64 + " >= 0 ", CommonData.numberEntities * 3},
                {CommonData.fieldInt64 + " < 10 ", 10},
                {CommonData.fieldInt64 + " != 10 ", CommonData.numberEntities * 3 - 1},
                {CommonData.fieldInt64 + " <= 10 ", 11},
                {"5<" + CommonData.fieldInt64 + " <= 10 ", 5},
                {CommonData.fieldInt64 + " >= 10 ", CommonData.numberEntities * 3 - 10},
                {CommonData.fieldInt64 + " > 100 ", CommonData.numberEntities * 3 - 101},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldBool + " == true", 2},
                {CommonData.fieldInt64 + " in [1,2,3] ", 3},
                {CommonData.fieldInt64 + " not in [1,2,3] ", CommonData.numberEntities * 3 - 3},
                {CommonData.fieldInt64 + " < 10 and " + CommonData.fieldInt32 + " >5 ", 2},
                {CommonData.fieldVarchar + " > \"0\" ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldVarchar + " like \"str%\" ", 0},
                {CommonData.fieldVarchar + " is null ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldVarchar + " is not null ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldVarchar + " like \"Str%\" ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldVarchar + " like \"Str1\" ", 0},
                {CommonData.fieldInt8 + " > 129 ", 0},
                {CommonData.fieldInt32 + " > 0 ", (CommonData.numberEntities * 3 / 2) - 1},
                {CommonData.fieldInt32 + " < 10 ", 5},
                {CommonData.fieldInt32 + " is null ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldInt32 + " is not null ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldInt32 + " != 10 ", (CommonData.numberEntities * 3 / 2) - 1},
                {CommonData.fieldInt32 + " <= 10 ", 6},
                {"5<" + CommonData.fieldInt32 + " <= 10 ", 3},
                {CommonData.fieldInt32 + " >= 10 ", (CommonData.numberEntities * 3 / 2) - 5},
                {CommonData.fieldInt32 + " > 100 ", (CommonData.numberEntities * 3 - 101) / 2},
                {CommonData.fieldInt32 + " < 10 and " + CommonData.fieldBool + " == true", 2},
                {CommonData.fieldInt32 + " in [1,2,3] ", 1},
                {CommonData.fieldInt32 + " not in [1,2,3] ", CommonData.numberEntities * 3 / 2 - 1},
                {CommonData.fieldInt32 + " < 10 and " + CommonData.fieldInt32 + " >5 ", 2},
                {CommonData.fieldFloat + "<= 10", 6},
                {CommonData.fieldArray + "[0] >= 0", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldArray + "[0] <= 10", 6},
                {"ARRAY_CONTAINS(" + CommonData.fieldArray + ", 1)", 1},
                {CommonData.fieldArray + " is null ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldArray + " is not null ", CommonData.numberEntities * 3 / 2},
                {(CommonData.numberEntities * 3 - 10) + " <= " + CommonData.fieldArray + "[0] ", 5},
                {CommonData.fieldArray + "[0] < 10 or " + (CommonData.numberEntities * 3 - 10) + " <= " + CommonData.fieldArray + "[0] ", 10},
                {CommonData.fieldArray + "[0] < 10 ||" + (CommonData.numberEntities * 3 - 10) + " <= " + CommonData.fieldArray + "[0] ", 10},
                {CommonData.fieldArray + "[0] not in [1,2,3]", (CommonData.numberEntities * 3 / 2) - 1},
                {CommonData.fieldArray + "[0]  in [1,2,3]", 1},
                {CommonData.fieldArray + "[0] != 0", (CommonData.numberEntities * 3 / 2) - 1},
                {CommonData.fieldArray + "[1] % 100 == 0 ", 0},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] < 10", 5},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] % 100 == 0", CommonData.numberEntities * 3 / 100},
                {CommonData.fieldJson + "['" + CommonData.fieldFloat + "'] < 1000 && " + CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] >= 500", 250},
                {CommonData.fieldJson + "['" + CommonData.fieldFloat + "'] < 10", 5},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] in [1,2,3]", 1},
                {CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] not in [1,2,3]", CommonData.numberEntities * 3 - 1},
                {"(" + CommonData.fieldJson + "['" + CommonData.fieldFloat + "'] < 10 || " + CommonData.fieldJson + "['" + CommonData.fieldInt64 + "'] >= " + (CommonData.numberEntities * 3 - 10) + ")", 10},
        };
    }

    @DataProvider(name = "samplingValue")
    public Object[][] providerSamplingValue() {
        return new Object[][]{
                {0.99}, {0.9}, {0.8}, {0.7}, {0.6}, {0.5}, {0.4}, {0.3}, {0.2}, {0.1}, {0.01}, {0.001}, {1}, {0}, {-0.1}, {2.5}, {65536}
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
                {Lists.newArrayList(CommonData.partitionNameA, CommonData.partitionNameB), "0 <= " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 2, CommonData.numberEntities * 2},
                {Lists.newArrayList(CommonData.partitionNameA, CommonData.partitionNameB, CommonData.partitionNameC), "0 <= " + CommonData.fieldInt64 + " <= " + CommonData.numberEntities * 3, CommonData.numberEntities * 3},
        };
    }

    @DataProvider(name = "DiffCollectionWithFilter")
    public Object[][] providerDiffCollectionWithFilter() {
        Object[][] vectorType = new Object[][]{
                {CommonData.defaultBFloat16VectorCollection},
                {CommonData.defaultBinaryVectorCollection},
                {CommonData.defaultFloat16VectorCollection},
                {CommonData.defaultSparseFloatVectorCollection}

        };
        Object[][] filter = new Object[][]{
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
        milvusClientV1 = new MilvusServiceClient(ConnectParam.newBuilder()
                .withUri(System.getProperty("uri") == null ? PropertyFilesUtil.getRunValue("uri") : System.getProperty("uri"))
                .withToken("root:Milvus")
                .build());
        nullableDefaultCollectionName = CommonFunction.createNewNullableDefaultValueCollection(CommonData.dim, null, DataType.FloatVector);
        // insert data
        List<JsonObject> jsonObjects = CommonFunction.generateSimpleNullData(0, CommonData.numberEntities, CommonData.dim, DataType.FloatVector);
        InsertResp insert = milvusClientV2.insert(InsertReq.builder().collectionName(nullableDefaultCollectionName).data(jsonObjects).build());
        CommonFunction.createVectorIndex(nullableDefaultCollectionName, CommonData.fieldFloatVector, IndexParam.IndexType.AUTOINDEX, IndexParam.MetricType.L2);
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
        CommonFunction.createPartition(nullableDefaultCollectionName, CommonData.partitionNameA);
        List<JsonObject> jsonObjectsA = CommonFunction.generateSimpleNullData(0, CommonData.numberEntities * 3, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(nullableDefaultCollectionName).partitionName(CommonData.partitionNameA).data(jsonObjectsA).build());
        // create samplingCollection
        samplingCollection = CommonFunction.createNewCollection(CommonData.dim, samplingCollection, DataType.FloatVector);
        CommonFunction.createIndexAndInsertAndLoad(samplingCollection, DataType.FloatVector, true, samplingCollectionEntityNum);
//        milvusClientV2.flush(FlushReq.builder().collectionNames(Lists.newArrayList(samplingCollection)).waitFlushedTimeoutMs(5000L).build());
        // create samplingCollection
       /* samplingCollectionWithMultiSegment = CommonFunction.createNewCollection(CommonData.dim, samplingCollectionWithMultiSegment, DataType.FloatVector);
        CommonFunction.createIndexAndInsertAndLoad(samplingCollection, DataType.FloatVector, true, samplingCollectionWithMultiSegmentEntityNum);
        milvusClientV2.flush(FlushReq.builder().collectionNames(Lists.newArrayList(samplingCollectionWithMultiSegment)).waitFlushedTimeoutMs(5000L).build());*/
        // create samplingNullableCollection
        samplingCollectionWithNullable = CommonFunction.createNewNullableCollection(CommonData.dim, null, DataType.FloatVector);
        List<JsonObject> generateSimpleNullData = CommonFunction.generateSimpleNullData(0, samplingCollectionWithNullableEntityNum, CommonData.dim, DataType.FloatVector);
        milvusClientV2.insert(InsertReq.builder().collectionName(samplingCollectionWithNullable).data(generateSimpleNullData).build());
        CommonFunction.createVectorIndex(samplingCollectionWithNullable, CommonData.fieldFloatVector, IndexParam.IndexType.AUTOINDEX, IndexParam.MetricType.L2);
        milvusClientV2.loadCollection(LoadCollectionReq.builder().collectionName(samplingCollectionWithNullable).build());

    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(nullableDefaultCollectionName).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(samplingCollection).build());
        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(samplingCollectionWithNullable).build());
//        milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(samplingCollectionWithMultiSegment).build());
    }

    @DataProvider(name = "queryNullableField")
    private Object[][] provideNullableFieldQueryParams() {
        return new Object[][]{
                {CommonData.fieldInt32 + " == 1 ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldDouble + " > 1 ", CommonData.numberEntities * 3 / 2 - 1},
                {CommonData.fieldVarchar + " == \"1.0\" ", CommonData.numberEntities * 3 / 2},
                {CommonData.fieldFloat + " == 1.0 ", CommonData.numberEntities * 3 / 2},
                {"fieldJson[\"" + CommonData.fieldVarchar + "\"] in [\"Str1\", \"Str3\"]", 0},
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


    @Test(description = "queryByAlias", groups = {"L1"})
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

    @Test(description = "queryInPartition", groups = {"L1"}, dataProvider = "queryPartition")
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

    @Test(description = "query with different collection", groups = {"L1"}, dataProvider = "DiffCollectionWithFilter")
    public void queryDiffCollection(String collectionName, String filter, long expect) {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .build());
        Assert.assertEquals(query.getQueryResults().size(), expect);
    }

    @Test(description = "query with nullable field", groups = {"L1"}, dataProvider = "queryNullableField")
    public void queryByNullFilter(String filter, long expect) {
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(nullableDefaultCollectionName)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Lists.newArrayList("*"))
                .filter(filter)
                .build());
        Assert.assertEquals(query.getQueryResults().size(), expect);
    }

    @Test(description = "sampling test", groups = {"L1"}, dataProvider = "filterAndExcept")
    public void samplingTest(String filter, long expect) {
//        // 查询collection的segment数量
//        int segmentSize = 0;
//        do {
//            R<GetQuerySegmentInfoResponse> querySegmentInfo =
//                    milvusClientV1.getQuerySegmentInfo(GetQuerySegmentInfoParam.newBuilder().withCollectionName(samplingCollection).build());
//            segmentSize = querySegmentInfo.getData().getInfosList().size();
//        } while (segmentSize == 0);
        double samplingRate = 0.1;
        String samplingFilter = "(" + filter + " )&& random_sample(" + samplingRate + ")";
        long samplingExpect;
        System.out.println("expect*samplingRate:" + expect * samplingRate);
        if (expect * samplingRate <= 1) {
            samplingExpect = 1;
        } else {
            samplingExpect = (long) (expect * samplingRate);
        }
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(samplingCollection)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .filter(samplingFilter)
                .build());
        System.out.println("expect:" + samplingExpect);
        System.out.println("actual:" + query.getQueryResults().size());
        Assert.assertTrue(query.getQueryResults().size() <= samplingExpect);
        Assert.assertTrue(query.getQueryResults().size() >= (samplingExpect - 1));
    }

    @Test(description = "sampling test with nullable value", groups = {"L1"}, dataProvider = "filterAndExceptWithNullable")
    public void samplingTestWithNullable(String filter, long expect) {
        double samplingRate = 0.1;
        String samplingFilter = "(" + filter + " )&& random_sample(" + samplingRate + ")";
        long samplingExpect;
        System.out.println("expect*samplingRate:" + expect * samplingRate);
        if (expect * samplingRate <= 1) {
            samplingExpect = 1;
        } else {
            samplingExpect = (long) (expect * samplingRate);
        }
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(samplingCollectionWithNullable)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .filter(samplingFilter)
                .build());
        System.out.println("expect:" + samplingExpect);
        System.out.println("actual:" + query.getQueryResults().size());
//        Assert.assertEquals(expect, query.getQueryResults().size());
        Assert.assertTrue(query.getQueryResults().size() <= samplingExpect);
        Assert.assertTrue(query.getQueryResults().size() >= (samplingExpect - 1));
    }

    @Test(description = "sampling test with limit", groups = {"L1"}, dataProvider = "filterAndExcept")
    public void samplingWithLimitTest(String filter, long expect) {
//        // 查询collection的segment数量
//        int segmentSize = 0;
//        do {
//            R<GetQuerySegmentInfoResponse> querySegmentInfo =
//                    milvusClientV1.getQuerySegmentInfo(GetQuerySegmentInfoParam.newBuilder().withCollectionName(samplingCollection).build());
//            segmentSize = querySegmentInfo.getData().getInfosList().size();
//        } while (segmentSize == 0);

        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(samplingCollection)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Lists.newArrayList("*"))
                .filter(filter)
                .limit(5)
                .build());
        System.out.println("expect:" + expect);
        System.out.println("actual:" + query.getQueryResults().size());
        expect = expect > 5 ? 5 : expect;
        Assert.assertTrue(query.getQueryResults().size() <= expect);
    }

    @Test(description = "query filter with different sampling value", groups = {"L1"}, dataProvider = "samplingValue")
    public void queryByFilterWithDifferentValue(double samplingValue) {
        String filter = CommonData.fieldInt64 + " >= 0 && random_sample(" + samplingValue + ") ";
        try {
            QueryResp query = milvusClientV2.query(QueryReq.builder()
                    .collectionName(samplingCollection)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Lists.newArrayList("*"))
                    .filter(filter)
                    .build());
            Assert.assertEquals(query.getQueryResults().size(), samplingCollectionEntityNum * samplingValue);
        } catch (MethodMatcherException e) {
            System.out.println("MethodMatcherException:" + e.getMessage());
        } catch (Exception e) {
            Assert.assertTrue(samplingValue >= 1 || samplingValue <= 0);
            Assert.assertTrue(e.getMessage().contains("should be between 0 and 1 and not too close to 0 or 1"));
        }
    }

    @Test(description = "query ids with different sampling value", groups = {"L1"})
    public void queryInIdsWithDifferentValue() {
        String filter = "random_sample(0.1) ";
        List<Object> ids = IntStream.range(0, 100)
                .boxed()
                .collect(Collectors.toList());
        try {
            QueryResp query = milvusClientV2.query(QueryReq.builder()
                    .collectionName(samplingCollection)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .outputFields(Lists.newArrayList("*"))
                    .ids(ids)
                    .filter(filter)
                    .build());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("can't be set at the same time"));
        }
    }


    @Test(description = "sampling test with multi segment", groups = {"L2"}, dataProvider = "samplingValue")
    public void samplingWithMultiSegment(double samplingValue) {

        // 查询collection的segment数量
        int segmentSize = 0;
        do {
            R<GetQuerySegmentInfoResponse> querySegmentInfo =
                    milvusClientV1.getQuerySegmentInfo(GetQuerySegmentInfoParam.newBuilder().withCollectionName(samplingCollectionWithMultiSegment).build());
            segmentSize = querySegmentInfo.getData().getInfosList().size();
        } while (segmentSize == 0);
        System.out.println("segmentSize: " + segmentSize);
        String filter = CommonData.fieldInt64 + " >= 0 && random_sample(" + samplingCollectionWithMultiSegmentEntityNum + ") ";
        QueryResp query = milvusClientV2.query(QueryReq.builder()
                .collectionName(samplingCollectionWithMultiSegment)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Lists.newArrayList("*"))
                .filter(filter)
                .build());
        Assert.assertEquals(query.getQueryResults().size(), samplingCollectionWithMultiSegmentEntityNum * samplingValue);
    }


}

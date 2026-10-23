package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.BoostRanker;
import io.milvus.v2.service.vector.request.ranker.DecayRanker;
import io.milvus.v2.service.vector.request.ranker.ModelRanker;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.request.ranker.WeightedRanker;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for ranker / FunctionScore of hybridSearch and search (Milvus 2.6)
 * Covers WeightedRanker / RRFRanker / DecayRanker / BoostRanker / ModelRanker
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class RankerFunctionScoreTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 2000;
    private static final String FIELD_VECTOR_1 = "floatVector1";
    private static final String FIELD_VECTOR_2 = "floatVector2";
    private static final String FIELD_NUMBER = "fieldNumber";

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "RankerFunction_" + GenerateUtil.getRandomString(6);

        // schema: Int64 pk + two FloatVector fields + one Int64 scalar field
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(FIELD_VECTOR_1)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(FIELD_VECTOR_2)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(FIELD_NUMBER)
                .dataType(DataType.Int64)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // insert data
        List<List<Float>> vectors1 = GenerateUtil.generateFloatVector(INSERT_COUNT, 6, DIM);
        List<List<Float>> vectors2 = GenerateUtil.generateFloatVector(INSERT_COUNT, 6, DIM);
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < INSERT_COUNT; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(FIELD_NUMBER, (long) i);
            JsonArray vec1 = new JsonArray();
            for (Float v : vectors1.get(i)) {
                vec1.add(v);
            }
            row.add(FIELD_VECTOR_1, vec1);
            JsonArray vec2 = new JsonArray();
            for (Float v : vectors2.get(i)) {
                vec2.add(v);
            }
            row.add(FIELD_VECTOR_2, vec2);
            data.add(row);
        }
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT);

        // HNSW index on both vector fields
        IndexParam indexParam1 = IndexParam.builder()
                .fieldName(FIELD_VECTOR_1)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .build();
        IndexParam indexParam2 = IndexParam.builder()
                .fieldName(FIELD_VECTOR_2)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Arrays.asList(indexParam1, indexParam2))
                .build());

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        }
    }

    // ==================== private helpers ====================

    private static List<AnnSearchReq> buildAnnSearchRequests(int nq, long limit) {
        List<AnnSearchReq> searchRequests = new ArrayList<>();
        List<BaseVector> vectors1 = new ArrayList<>();
        for (List<Float> vec : GenerateUtil.generateFloatVector(nq, 6, DIM)) {
            vectors1.add(new FloatVec(vec));
        }
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName(FIELD_VECTOR_1)
                .vectors(vectors1)
                .limit(limit)
                .build());
        List<BaseVector> vectors2 = new ArrayList<>();
        for (List<Float> vec : GenerateUtil.generateFloatVector(nq, 6, DIM)) {
            vectors2.add(new FloatVec(vec));
        }
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName(FIELD_VECTOR_2)
                .vectors(vectors2)
                .limit(limit)
                .build());
        return searchRequests;
    }

    private static List<BaseVector> buildSearchData(int nq) {
        List<BaseVector> vectors = new ArrayList<>();
        for (List<Float> vec : GenerateUtil.generateFloatVector(nq, 6, DIM)) {
            vectors.add(new FloatVec(vec));
        }
        return vectors;
    }

    // ==================== Positive cases ====================

    @Test(description = "hybridSearch with WeightedRanker via functionScore", groups = {"Smoke"})
    public void hybridSearchWithWeightedRanker() {
        WeightedRanker weightedRanker = WeightedRanker.builder()
                .name("wr")
                .weights(Arrays.asList(0.7f, 0.3f))
                .build();
        SearchResp searchResp = milvusClientV2.hybridSearch(HybridSearchReq.builder()
                .collectionName(collectionName)
                .searchRequests(buildAnnSearchRequests(1, 10))
                .functionScore(FunctionScore.builder().addFunction(weightedRanker).build())
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
    }

    @Test(description = "hybridSearch with RRFRanker (regression of legacy ranker)", groups = {"Smoke"})
    public void hybridSearchWithRRFRanker() {
        SearchResp searchResp = milvusClientV2.hybridSearch(HybridSearchReq.builder()
                .collectionName(collectionName)
                .searchRequests(buildAnnSearchRequests(1, 10))
                .ranker(new RRFRanker(60))
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
    }

    @DataProvider(name = "decayFunctionProvider")
    public Object[][] provideDecayFunctions() {
        return new Object[][]{
                {"gauss"},
                {"exp"},
                {"linear"}
        };
    }

    @Test(description = "hybridSearch with DecayRanker of gauss/exp/linear function", groups = {"Smoke"}, dataProvider = "decayFunctionProvider")
    public void hybridSearchWithDecayRanker(String decayFunction) {
        DecayRanker decayRanker = DecayRanker.builder()
                .name("dr")
                .function(decayFunction)
                .inputFieldNames(Collections.singletonList(FIELD_NUMBER))
                .origin(100)
                .scale(10)
                .offset(0)
                .decay(0.5)
                .build();
        SearchResp searchResp = milvusClientV2.hybridSearch(HybridSearchReq.builder()
                .collectionName(collectionName)
                .searchRequests(buildAnnSearchRequests(1, 10))
                .functionScore(FunctionScore.builder().addFunction(decayRanker).build())
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
    }

    // 已确认 hybridSearch + BoostRanker 在 Milvus 2.6-20261022-2775715f9f 上存在问题：
    // FunctionScore 路径导致服务端进程崩溃，.ranker() 单 ranker 路径导致请求服务端挂起不返回；
    // 最小复现见 vectorOperation/BoostRankerCrashIssue.java（hybrid / hybrid-ranker 模式），服务端修复前禁用
    @Test(description = "hybridSearch with BoostRanker via functionScore", groups = {"Smoke"}, enabled = false)
    public void hybridSearchWithBoostRanker() {
        BoostRanker boostRanker = BoostRanker.builder()
                .name("br")
                .filter(FIELD_NUMBER + " > 500")
                .weight(2.0f)
                .build();
        SearchResp searchResp = milvusClientV2.hybridSearch(HybridSearchReq.builder()
                .collectionName(collectionName)
                .searchRequests(buildAnnSearchRequests(1, 10))
                .functionScore(FunctionScore.builder().addFunction(boostRanker).build())
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
    }

    // 含 BoostRanker，同上的服务端崩溃问题，修复前禁用
    @Test(description = "hybridSearch with FunctionScore combining two rankers", groups = {"Smoke"}, enabled = false)
    public void hybridSearchWithMultiRankerFunctionScore() {
        WeightedRanker weightedRanker = WeightedRanker.builder()
                .name("wr")
                .weights(Arrays.asList(0.7f, 0.3f))
                .build();
        BoostRanker boostRanker = BoostRanker.builder()
                .name("br")
                .filter(FIELD_NUMBER + " > 500")
                .weight(2.0f)
                .build();
        FunctionScore functionScore = FunctionScore.builder()
                .addFunction(weightedRanker)
                .addFunction(boostRanker)
                .build();
        SearchResp searchResp = milvusClientV2.hybridSearch(HybridSearchReq.builder()
                .collectionName(collectionName)
                .searchRequests(buildAnnSearchRequests(1, 10))
                .functionScore(functionScore)
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
    }

    // search + FunctionScore(BoostRanker) 已在 2.6-20261022-2775715f9f 上验证正常（vectorOperation/BoostRankerCrashIssue.java search 模式）
    @Test(description = "single vector search with FunctionScore(BoostRanker)", groups = {"Smoke"})
    public void searchWithFunctionScore() {
        BoostRanker boostRanker = BoostRanker.builder()
                .name("br")
                .filter(FIELD_NUMBER + " > 500")
                .weight(2.0f)
                .build();
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(FIELD_VECTOR_1)
                .data(buildSearchData(1))
                .functionScore(FunctionScore.builder().addFunction(boostRanker).build())
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10);
    }

    // ==================== Negative cases ====================

    @Test(description = "Set both ranker and functionScore should fail", groups = {"Smoke"})
    public void setBothRankerAndFunctionScoreShouldFail() {
        try {
            milvusClientV2.hybridSearch(HybridSearchReq.builder()
                    .collectionName(collectionName)
                    .searchRequests(buildAnnSearchRequests(1, 10))
                    .ranker(new RRFRanker(60))
                    .functionScore(FunctionScore.builder()
                            .addFunction(WeightedRanker.builder()
                                    .name("wr")
                                    .weights(Arrays.asList(0.7f, 0.3f))
                                    .build())
                            .build())
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when both ranker and functionScore are set");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Not allow to set both"),
                    "Exception message should mention mutual exclusion: " + e.getMessage());
        }
    }

    @Test(description = "WeightedRanker weights count mismatches searchRequests count should fail", groups = {"Smoke"})
    public void weightedRankerWeightsCountMismatchShouldFail() {
        try {
            // 2 search requests but 3 weights
            WeightedRanker weightedRanker = WeightedRanker.builder()
                    .name("wr")
                    .weights(Arrays.asList(0.5f, 0.3f, 0.2f))
                    .build();
            milvusClientV2.hybridSearch(HybridSearchReq.builder()
                    .collectionName(collectionName)
                    .searchRequests(buildAnnSearchRequests(1, 10))
                    .functionScore(FunctionScore.builder().addFunction(weightedRanker).build())
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when weights count mismatches search requests count");
        } catch (Exception e) {
            // expected exception
        }
    }

    @Test(description = "DecayRanker with non-existent input field should fail", groups = {"Smoke"})
    public void decayRankerWithNonExistentFieldShouldFail() {
        try {
            DecayRanker decayRanker = DecayRanker.builder()
                    .name("dr")
                    .function("gauss")
                    .inputFieldNames(Collections.singletonList("notExistField"))
                    .origin(100)
                    .scale(10)
                    .offset(0)
                    .decay(0.5)
                    .build();
            milvusClientV2.hybridSearch(HybridSearchReq.builder()
                    .collectionName(collectionName)
                    .searchRequests(buildAnnSearchRequests(1, 10))
                    .functionScore(FunctionScore.builder().addFunction(decayRanker).build())
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when decay ranker input field does not exist");
        } catch (Exception e) {
            // expected exception
        }
    }

    @Test(description = "ModelRanker with unreachable tei endpoint should fail", groups = {"Smoke"})
    public void modelRankerUnreachableEndpointShouldFail() {
        try {
            // there is no external model service in test env, endpoint is unreachable on purpose
            ModelRanker modelRanker = ModelRanker.builder()
                    .name("mr")
                    .provider("tei")
                    .queries(Collections.singletonList("milvus ranker test"))
                    .endpoint("http://127.0.0.1:1/tei")
                    .build();
            milvusClientV2.hybridSearch(HybridSearchReq.builder()
                    .collectionName(collectionName)
                    .searchRequests(buildAnnSearchRequests(1, 10))
                    .functionScore(FunctionScore.builder().addFunction(modelRanker).build())
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when model ranker endpoint is unreachable");
        } catch (Exception e) {
            // expected exception
        }
    }
}

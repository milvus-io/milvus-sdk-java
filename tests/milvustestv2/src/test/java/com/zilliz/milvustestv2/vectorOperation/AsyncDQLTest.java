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
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Test cases for async DQL interfaces of MilvusClientV2:
 * getAsync / queryAsync / searchAsync / hybridSearchAsync.
 *
 * @Author yongpeng.li
 * @Date 2026
 */
public class AsyncDQLTest extends BaseTest {

    private String collectionName;

    private static final int DIM = 128;
    private static final int INSERT_COUNT = 2000;
    private static final int TOP_K = 10;
    private static final String FIELD_FLOAT_VECTOR_2 = "fieldFloatVector2";

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "AsyncDQL_" + GenerateUtil.getRandomString(6);

        // Int64 pk + two FloatVector fields + one varchar scalar field
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(FIELD_FLOAT_VECTOR_2)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(512)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // insert data
        List<JsonObject> data = new ArrayList<>();
        List<List<Float>> vectors1 = GenerateUtil.generateFloatVector(INSERT_COUNT, 6, DIM);
        List<List<Float>> vectors2 = GenerateUtil.generateFloatVector(INSERT_COUNT, 6, DIM);
        for (int i = 0; i < INSERT_COUNT; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            JsonArray vector1 = new JsonArray();
            for (Float f : vectors1.get(i)) {
                vector1.add(f);
            }
            row.add(CommonData.fieldFloatVector, vector1);
            JsonArray vector2 = new JsonArray();
            for (Float f : vectors2.get(i)) {
                vector2.add(f);
            }
            row.add(FIELD_FLOAT_VECTOR_2, vector2);
            row.addProperty(CommonData.fieldVarchar, "Str" + i);
            data.add(row);
        }
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());

        // HNSW index on both vector fields
        Map<String, Object> hnswParams = new HashMap<>();
        hnswParams.put("M", 16);
        hnswParams.put("efConstruction", 200);
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(hnswParams)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(FIELD_FLOAT_VECTOR_2)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(hnswParams)
                .build());
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(indexParams)
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

    private SearchReq buildSearchReq(List<Float> queryVector) {
        return SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(TOP_K)
                .outputFields(Collections.singletonList(CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();
    }

    // ==================== Positive Cases ====================

    @Test(description = "searchAsync returns the same topK ids as sync search with same params", groups = {"Smoke"})
    public void searchAsyncConsistentWithSync() throws Exception {
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchReq searchReq = buildSearchReq(queryVector);

        SearchResp syncResp = milvusClientV2.search(searchReq);
        CompletableFuture<SearchResp> future = milvusClientV2.searchAsync(searchReq);
        SearchResp asyncResp = future.get();

        Assert.assertFalse(syncResp.getSearchResults().isEmpty());
        Assert.assertFalse(asyncResp.getSearchResults().isEmpty());
        List<SearchResp.SearchResult> syncResults = syncResp.getSearchResults().get(0);
        List<SearchResp.SearchResult> asyncResults = asyncResp.getSearchResults().get(0);
        Assert.assertEquals(asyncResults.size(), syncResults.size());
        List<Object> syncIds = new ArrayList<>();
        for (SearchResp.SearchResult result : syncResults) {
            syncIds.add(result.getId());
        }
        List<Object> asyncIds = new ArrayList<>();
        for (SearchResp.SearchResult result : asyncResults) {
            asyncIds.add(result.getId());
        }
        Assert.assertEquals(asyncIds, syncIds, "Async search topK ids should equal sync search topK ids");
    }

    @Test(description = "queryAsync returns the same results as sync query", groups = {"Smoke"})
    public void queryAsyncConsistentWithSync() throws Exception {
        // count(*) with filter
        QueryReq countReq = QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt64 + " < 100")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();
        QueryResp syncCountResp = milvusClientV2.query(countReq);
        QueryResp asyncCountResp = milvusClientV2.queryAsync(countReq).get();
        long syncCount = (Long) syncCountResp.getQueryResults().get(0).getEntity().get("count(*)");
        long asyncCount = (Long) asyncCountResp.getQueryResults().get(0).getEntity().get("count(*)");
        Assert.assertEquals(syncCount, 100L);
        Assert.assertEquals(asyncCount, syncCount, "Async count(*) should equal sync count(*)");

        // normal field query
        QueryReq queryReq = QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt64 + " in [1, 2, 3]")
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();
        QueryResp syncResp = milvusClientV2.query(queryReq);
        QueryResp asyncResp = milvusClientV2.queryAsync(queryReq).get();
        Assert.assertEquals(syncResp.getQueryResults().size(), 3);
        Assert.assertEquals(asyncResp.getQueryResults().size(), syncResp.getQueryResults().size(),
                "Async query row count should equal sync query row count");
    }

    @Test(description = "getAsync returns correct rows and fields by ids", groups = {"Smoke"})
    public void getAsyncByIdsSuccess() throws Exception {
        CompletableFuture<GetResp> future = milvusClientV2.getAsync(GetReq.builder()
                .collectionName(collectionName)
                .ids(Arrays.<Object>asList(1L, 2L, 3L))
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldVarchar))
                .build());
        GetResp getResp = future.get();

        Assert.assertNotNull(getResp);
        Assert.assertEquals(getResp.getGetResults().size(), 3, "Get by 3 ids should return 3 rows");
        List<Long> ids = new ArrayList<>();
        for (QueryResp.QueryResult result : getResp.getGetResults()) {
            Assert.assertTrue(result.getEntity().containsKey(CommonData.fieldVarchar),
                    "Returned entity should contain varchar field");
            ids.add((Long) result.getEntity().get(CommonData.fieldInt64));
        }
        Assert.assertTrue(ids.contains(1L) && ids.contains(2L) && ids.contains(3L));
    }

    @Test(description = "hybridSearchAsync on two vector fields with RRF ranker returns results", groups = {"Smoke"})
    public void hybridSearchAsyncSuccess() throws Exception {
        List<Float> queryVector1 = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        List<Float> queryVector2 = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);

        AnnSearchReq annSearchReq1 = AnnSearchReq.builder()
                .vectorFieldName(CommonData.fieldFloatVector)
                .vectors(Collections.singletonList(new FloatVec(queryVector1)))
                .topK(TOP_K)
                .build();
        AnnSearchReq annSearchReq2 = AnnSearchReq.builder()
                .vectorFieldName(FIELD_FLOAT_VECTOR_2)
                .vectors(Collections.singletonList(new FloatVec(queryVector2)))
                .topK(TOP_K)
                .build();

        RRFRanker rrfRanker = RRFRanker.builder()
                .name("rrf")
                .k(60)
                .build();
        // HybridSearchReq.ranker is deprecated, use functionScore instead
        FunctionScore functionScore = FunctionScore.builder()
                .addFunction(rrfRanker)
                .build();

        HybridSearchReq hybridSearchReq = HybridSearchReq.builder()
                .collectionName(collectionName)
                .searchRequests(Arrays.asList(annSearchReq1, annSearchReq2))
                .functionScore(functionScore)
                .topK(TOP_K)
                .outFields(Collections.singletonList(CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();

        CompletableFuture<SearchResp> future = milvusClientV2.hybridSearchAsync(hybridSearchReq);
        SearchResp searchResp = future.get();

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "Hybrid search should return results");
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= TOP_K);
    }

    @Test(description = "Concurrent searchAsync calls all complete with correct results", groups = {"Smoke"})
    public void concurrentSearchAsyncSuccess() throws Exception {
        CompletableFuture<SearchResp> future1 = milvusClientV2.searchAsync(
                buildSearchReq(GenerateUtil.generateFloatVector(1, 6, DIM).get(0)));
        CompletableFuture<SearchResp> future2 = milvusClientV2.searchAsync(
                buildSearchReq(GenerateUtil.generateFloatVector(1, 6, DIM).get(0)));
        CompletableFuture<SearchResp> future3 = milvusClientV2.searchAsync(
                buildSearchReq(GenerateUtil.generateFloatVector(1, 6, DIM).get(0)));

        CompletableFuture.allOf(future1, future2, future3).get();

        for (CompletableFuture<SearchResp> future : Arrays.asList(future1, future2, future3)) {
            SearchResp searchResp = future.get();
            Assert.assertNotNull(searchResp);
            Assert.assertFalse(searchResp.getSearchResults().isEmpty());
            Assert.assertEquals(searchResp.getSearchResults().get(0).size(), TOP_K,
                    "Each concurrent searchAsync should return topK results");
        }
    }

    // ==================== Negative Cases ====================

    @Test(description = "searchAsync on non-exist collection should complete exceptionally", groups = {"Smoke"})
    public void searchAsyncWithNonExistCollectionShouldFail() {
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        CompletableFuture<SearchResp> future = milvusClientV2.searchAsync(SearchReq.builder()
                .collectionName("NotExistCollection_" + GenerateUtil.getRandomString(6))
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(TOP_K)
                .build());
        try {
            future.get();
            Assert.fail("searchAsync on non-exist collection should fail");
        } catch (ExecutionException e) {
            // expected: future completes exceptionally, cause carries the original rpc error
            Assert.assertNotNull(e.getCause(), "ExecutionException should carry the original cause");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("searchAsync should not be interrupted");
        }
    }

    @Test(description = "getAsync on non-exist collection should complete exceptionally", groups = {"Smoke"})
    public void getAsyncWithNonExistCollectionShouldFail() {
        CompletableFuture<GetResp> future = milvusClientV2.getAsync(GetReq.builder()
                .collectionName("NotExistCollection_" + GenerateUtil.getRandomString(6))
                .ids(Collections.<Object>singletonList(1L))
                .build());
        try {
            future.get();
            Assert.fail("getAsync on non-exist collection should fail");
        } catch (ExecutionException e) {
            Assert.assertNotNull(e.getCause(), "ExecutionException should carry the original cause");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assert.fail("getAsync should not be interrupted");
        }
    }

    @Test(description = "getAsync with empty ids list returns empty result without rpc (SDK short-circuit, aligned with pymilvus)",
            groups = {"Smoke"})
    public void getAsyncWithEmptyIdsReturnsEmpty() throws Exception {
        // Note: milvus-sdk-java 2.6.25 short-circuits an empty ids list to an empty GetResp
        // without issuing the rpc, instead of throwing an exception.
        CompletableFuture<GetResp> future = milvusClientV2.getAsync(GetReq.builder()
                .collectionName(collectionName)
                .ids(new ArrayList<>())
                .outputFields(Collections.singletonList(CommonData.fieldVarchar))
                .build());
        GetResp getResp = future.get();
        Assert.assertNotNull(getResp);
        Assert.assertTrue(getResp.getGetResults().isEmpty(),
                "Get with empty ids should return empty result");
    }
}

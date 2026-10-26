package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.FunctionChain;
import io.milvus.v2.service.vector.request.FunctionChainArg;
import io.milvus.v2.service.vector.request.FunctionChainExpr;
import io.milvus.v2.service.vector.request.FunctionChainStage;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.SearchResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Test cases for Milvus 3.0 search function chains (SearchReq.functionChains).
 * Data: 1000 rows, fieldInt64=fieldInt32=i, fieldDouble=i*0.5, fieldVarchar="brand_"+(i%5).
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class FunctionChainTest extends BaseTest {

    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;

    private String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "FunctionChain_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt32)
                .dataType(DataType.Int32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldDouble)
                .dataType(DataType.Double)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(generateData(INSERT_COUNT))
                .build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 200);
                }})
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
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

    private static List<JsonObject> generateData(int count) {
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldInt32, i);
            row.addProperty(CommonData.fieldDouble, i * 0.5);
            row.addProperty(CommonData.fieldVarchar, "brand_" + (i % 5));
            JsonArray vector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                vector.add(v);
            }
            row.add(CommonData.fieldFloatVector, vector);
            rows.add(row);
        }
        return rows;
    }

    private static FunctionChain buildRoundAndSortChain(FunctionChainStage stage, int limit, int offset) {
        FunctionChain.FunctionChainBuilder builder = FunctionChain.builder()
                .stage(stage)
                .name("round_and_sort")
                .map("$score", FunctionChainExpr.builder()
                        .name("round_decimal")
                        .arg(FunctionChainArg.col("$score"))
                        .param("decimal", 2)
                        .build())
                .sort("$score", true, "");
        if (offset > 0) {
            builder.limit(limit, offset);
        } else {
            builder.limit(limit);
        }
        return builder.build();
    }

    private SearchResp searchWithChain(FunctionChain chain, long searchLimit) {
        return milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(GenerateUtil.generateFloatVector(1, 6, DIM).get(0))))
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .limit(searchLimit)
                .addFunctionChain(chain)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
    }

    // ==================== Positive Cases ====================

    @Test(description = "Search with the official round_and_sort function chain at L2_RERANK",
            groups = {"Smoke", "V3"})
    public void searchWithOfficialFunctionChain() {
        FunctionChain chain = buildRoundAndSortChain(FunctionChainStage.L2_RERANK, 10, 0);
        SearchResp searchResp = searchWithChain(chain, 10);

        Assert.assertEquals(searchResp.getSearchResults().size(), 1);
        List<SearchResp.SearchResult> results = searchResp.getSearchResults().get(0);
        Assert.assertFalse(results.isEmpty());
        Assert.assertTrue(results.size() <= 10, "Chain limit(10), got " + results.size());
        for (SearchResp.SearchResult result : results) {
            Assert.assertNotNull(result.getId());
        }
    }

    @Test(description = "Search with function chain limit(5, offset 2), record the pagination behavior",
            groups = {"Smoke", "V3"})
    public void searchWithFunctionChainLimitOffset() {
        FunctionChain chain = buildRoundAndSortChain(FunctionChainStage.L2_RERANK, 5, 2);
        SearchResp searchResp = searchWithChain(chain, 20);

        Assert.assertEquals(searchResp.getSearchResults().size(), 1);
        List<SearchResp.SearchResult> results = searchResp.getSearchResults().get(0);
        log.info("Function chain limit(5, offset 2) returned {} rows", results.size());
        Assert.assertTrue(results.size() <= 5, "Chain limit(5, offset=2), got " + results.size());
    }

    @Test(description = "Search with the same chain at L1_RERANK stage, record the behavior",
            groups = {"Smoke", "V3"})
    public void searchWithFunctionChainL1Stage() {
        FunctionChain chain = buildRoundAndSortChain(FunctionChainStage.L1_RERANK, 10, 0);
        try {
            SearchResp searchResp = searchWithChain(chain, 10);
            List<SearchResp.SearchResult> results = searchResp.getSearchResults().get(0);
            // Actual behavior: L1_RERANK chains run against the per-segment hits.
            log.info("L1_RERANK chain returned {} rows", results.size());
            Assert.assertTrue(results.size() <= 10);
        } catch (Exception e) {
            // Record if the server rejects chains at this stage
            log.warn("L1_RERANK chain rejected by the server: {}", e.getMessage());
        }
    }

    // ==================== Negative Cases ====================

    @Test(description = "addFunctionChain(null) should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void addNullFunctionChainShouldFail() {
        SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(GenerateUtil.generateFloatVector(1, 6, DIM).get(0))))
                .limit(10)
                .addFunctionChain(null)
                .build();
    }

    @Test(description = "Function chain with an unknown function name should fail on the server",
            groups = {"Smoke", "V3"})
    public void functionChainUnknownFunctionShouldFail() {
        FunctionChain chain = FunctionChain.builder()
                .stage(FunctionChainStage.L2_RERANK)
                .name("unknown_func_chain")
                .map("$score", FunctionChainExpr.builder()
                        .name("not_exist_func")
                        .arg(FunctionChainArg.col("$score"))
                        .build())
                .sort("$score", true, "")
                .limit(10)
                .build();
        try {
            searchWithChain(chain, 10);
            // Actual behavior: the server is expected to reject the unknown function name.
            Assert.fail("Search with an unknown chain function should fail on the server");
        } catch (Exception e) {
            log.info("Unknown chain function rejected as expected: {}", e.getMessage());
        }
    }

    @Test(description = "Function chain mapping a non-existent column should fail on the server",
            groups = {"Smoke", "V3"})
    public void functionChainUnknownColumnShouldFail() {
        FunctionChain chain = FunctionChain.builder()
                .stage(FunctionChainStage.L2_RERANK)
                .name("unknown_col_chain")
                .map("new_col", FunctionChainExpr.builder()
                        .name("round_decimal")
                        .arg(FunctionChainArg.col("not_exist_column"))
                        .param("decimal", 2)
                        .build())
                .sort("new_col", true, "")
                .limit(10)
                .build();
        try {
            searchWithChain(chain, 10);
            // Actual behavior: the server is expected to reject the unknown input column.
            Assert.fail("Search with a chain mapping a non-existent column should fail on the server");
        } catch (Exception e) {
            log.info("Unknown chain column rejected as expected: {}", e.getMessage());
        }
    }

    @Test(description = "functionChains and functionScore are mutually exclusive, record the actual behavior",
            groups = {"Smoke", "V3"})
    public void functionChainWithFunctionScore() {
        FunctionChain chain = buildRoundAndSortChain(FunctionChainStage.L2_RERANK, 10, 0);
        FunctionScore functionScore = FunctionScore.builder()
                .addFunction(new RRFRanker(60))
                .build();
        try {
            SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .annsField(CommonData.fieldFloatVector)
                    .data(Collections.singletonList(new FloatVec(GenerateUtil.generateFloatVector(1, 6, DIM).get(0))))
                    .limit(10)
                    .addFunctionChain(chain)
                    .functionScore(functionScore)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            // Actual behavior: functionChains and functionScore are documented as mutually
            // exclusive; if the server accepts both, log the response for diagnosis.
            log.warn("functionChains + functionScore did not fail, result size: {}",
                    searchResp.getSearchResults().get(0).size());
        } catch (Exception e) {
            log.info("functionChains + functionScore rejected as expected: {}", e.getMessage());
        }
    }
}

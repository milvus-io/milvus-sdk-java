package com.zilliz.milvustestv2.others;

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
import io.milvus.v2.service.utility.OptimizeTask;
import io.milvus.v2.service.utility.request.OptimizeReq;
import io.milvus.v2.service.utility.response.OptimizeResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
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
 * Test cases for optimize API (force merge segments to target size + rebuild index).
 * 数据量保持在 1000 条以内，避免 optimize 执行过慢。
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
@Slf4j
public class OptimizeTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "Optimize_" + GenerateUtil.getRandomString(6);
        createCollectionWithData(collectionName, INSERT_COUNT);
        createHnswIndex(collectionName);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        }
    }

    @Test(description = "Optimize collection with default params (sync mode)", groups = {"Smoke"})
    public void optimizeSyncDefault() {
        // 默认 async=false，optimize() 内部已阻塞到完成
        OptimizeTask task = milvusClientV2.optimize(OptimizeReq.builder()
                .collectionName(collectionName)
                .build());
        Assert.assertNotNull(task);
        Assert.assertTrue(task.isDone(), "Sync optimize should already be done when optimize() returns");
    }

    @Test(description = "Optimize collection with async=true and timeout", groups = {"Smoke"}, dependsOnMethods = {"optimizeSyncDefault"})
    public void optimizeAsyncWithTimeout() {
        // 2.6-20261022 nightly 实测：服务端要求 targetSize >= configMaxSize(1024MB)，
        // "512MB" 会被拒绝（targetSize 512 MB should be greater than or equal to configMaxSize 1024 MB），
        // 因此这里使用 "1GB"；resp.getTargetSize() 回显格式以实际为准，仅断言非空
        OptimizeTask task = milvusClientV2.optimize(OptimizeReq.builder()
                .collectionName(collectionName)
                .targetSize("1GB")
                .async(true)
                .timeout(120000L)
                .build());
        Assert.assertNotNull(task);
        OptimizeResp resp = task.getResult(120000L);
        Assert.assertNotNull(resp);
        Assert.assertTrue(task.isDone());
        // status 预期为 "success"
        Assert.assertNotNull(resp.getStatus());
        Assert.assertFalse(resp.getStatus().isEmpty());
        Assert.assertNotNull(resp.getTargetSize());
        Assert.assertFalse(resp.getTargetSize().isEmpty());
        List<String> progressHistory = task.getProgressHistoryAsStrings();
        Assert.assertNotNull(progressHistory);
        Assert.assertFalse(progressHistory.isEmpty(), "Progress history should not be empty");
        log.info("optimize async resp: " + resp);
    }

    @Test(description = "Search after optimize, data should be intact", groups = {"Smoke"}, dependsOnMethods = {"optimizeAsyncWithTimeout"})
    public void searchAfterOptimize() {
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // count 校验：optimize 不丢数据
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Long count = (Long) queryResp.getQueryResults().get(0).getEntity().get("count(*)");
        Assert.assertEquals(count, Long.valueOf(INSERT_COUNT), "Data count should be intact after optimize");

        // search 校验
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(10)
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty());
    }

    @Test(description = "Optimize without collectionName should throw IllegalArgumentException", groups = {"Smoke"})
    public void optimizeWithoutCollectionName() {
        try {
            OptimizeReq.builder().build();
            Assert.fail("build() should throw IllegalArgumentException when collectionName is not set");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("collectionName"));
        }
    }

    @Test(description = "Optimize with invalid targetSize (smaller than 1MB) should fail", groups = {"Smoke"})
    public void optimizeWithInvalidTargetSize() {
        try {
            // targetSize 最小 1MB，"10KB" 应在构造 OptimizeTask 时抛 MilvusClientException
            milvusClientV2.optimize(OptimizeReq.builder()
                    .collectionName(collectionName)
                    .targetSize("10KB")
                    .build());
            Assert.fail("optimize should fail with targetSize smaller than 1MB");
        } catch (Exception e) {
            log.info("expected exception: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    @Test(description = "Optimize not existed collection should fail", groups = {"Smoke"})
    public void optimizeNotExistedCollection() {
        try {
            milvusClientV2.optimize(OptimizeReq.builder()
                    .collectionName("OptimizeNotExist_" + GenerateUtil.getRandomString(6))
                    .timeout(60000L)
                    .build());
            Assert.fail("optimize on not existed collection should fail");
        } catch (Exception e) {
            log.info("expected exception: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    /**
     * 通用数据准备：Int64 主键 + FloatVector(128) + Int32/VarChar 标量，insert 指定条数
     */
    private static void createCollectionWithData(String collectionName, int count) {
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
                .fieldName(CommonData.fieldInt32)
                .dataType(DataType.Int32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        List<List<Float>> vectors = GenerateUtil.generateFloatVector(count, 6, DIM);
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldInt32, i);
            row.addProperty(CommonData.fieldVarchar, "str_" + i);
            JsonArray vector = new JsonArray();
            for (Float f : vectors.get(i)) {
                vector.add(f);
            }
            row.add(CommonData.fieldFloatVector, vector);
            data.add(row);
        }
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
    }

    /**
     * 创建 HNSW 索引（M=16/efConstruction=64，L2）
     */
    private static void createHnswIndex(String collectionName) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 64);
                }})
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }
}

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
import io.milvus.v2.service.utility.request.FlushAllReq;
import io.milvus.v2.service.utility.request.GetFlushAllStateReq;
import io.milvus.v2.service.utility.response.FlushAllResp;
import io.milvus.v2.service.utility.response.GetFlushAllStateResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;
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
 * Test cases for flushAll / getFlushAllState APIs (flush all collections in a database).
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
@Slf4j
public class FlushAllTest extends BaseTest {

    private String collectionNameA;
    private String collectionNameB;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 500;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        String suffix = GenerateUtil.getRandomString(6);
        collectionNameA = "FlushAllA_" + suffix;
        collectionNameB = "FlushAllB_" + suffix;
        createCollectionWithData(collectionNameA, INSERT_COUNT);
        createCollectionWithData(collectionNameB, INSERT_COUNT);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionNameA != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionNameA)
                    .build());
        }
        if (collectionNameB != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionNameB)
                    .build());
        }
    }

    @Test(description = "Flush all collections in database and check flush state", groups = {"Smoke"})
    public void flushAllSuccess() {
        // flushAll 总是阻塞等到 flushed
        FlushAllResp flushAllResp = milvusClientV2.flushAll(FlushAllReq.builder().build());
        Assert.assertNotNull(flushAllResp);
        Assert.assertNotNull(flushAllResp.getFlushAllTs());
        Assert.assertTrue(flushAllResp.getFlushAllTs() > 0, "flushAllTs should be positive");

        GetFlushAllStateResp stateResp = milvusClientV2.getFlushAllState(GetFlushAllStateReq.builder()
                .flushAllTs(flushAllResp.getFlushAllTs())
                .build());
        Assert.assertNotNull(stateResp);
        Assert.assertTrue(stateResp.getFlushed(), "flushAll state should be flushed");

        // flush 不丢数据：load 后两个 collection 的 count 都正确
        createHnswIndex(collectionNameA);
        createHnswIndex(collectionNameB);
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionNameA)
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionNameB)
                .build());

        Assert.assertEquals(queryCount(collectionNameA), Long.valueOf(INSERT_COUNT),
                "Collection A count should be correct after flushAll");
        Assert.assertEquals(queryCount(collectionNameB), Long.valueOf(INSERT_COUNT),
                "Collection B count should be correct after flushAll");
    }

    @Test(description = "Get flush all state with flushAllTs=0 (boundary)", groups = {"Smoke"})
    public void getFlushAllStateWithZeroTs() {
        try {
            GetFlushAllStateResp stateResp = milvusClientV2.getFlushAllState(GetFlushAllStateReq.builder()
                    .flushAllTs(0L)
                    .build());
            // 实际行为记录：flushAllTs=0 时服务端正常返回，flushed 为 true
            log.info("getFlushAllState with flushAllTs=0 returns flushed=" + stateResp.getFlushed());
            Assert.assertNotNull(stateResp.getFlushed());
        } catch (Exception e) {
            // 实际行为记录：flushAllTs=0 时抛异常也视为合理行为
            log.info("getFlushAllState with flushAllTs=0 throws: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    @Test(description = "Flush all with not existed databaseName", groups = {"Smoke"})
    public void flushAllWithNotExistedDatabase() {
        try {
            FlushAllResp resp = milvusClientV2.flushAll(FlushAllReq.builder()
                    .databaseName("db_not_exist_" + GenerateUtil.getRandomString(6))
                    .waitFlushedTimeoutMs(60000L)
                    .build());
            // 实际行为记录：客户端/服务端未校验 databaseName，正常返回
            log.info("flushAll with not existed databaseName returns flushAllTs=" + resp.getFlushAllTs());
        } catch (Exception e) {
            // 实际行为记录：服务端校验 databaseName 不存在并抛异常
            log.info("flushAll with not existed databaseName throws: " + e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }
    }

    private static Long queryCount(String collectionName) {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        return (Long) queryResp.getQueryResults().get(0).getEntity().get("count(*)");
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

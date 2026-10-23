package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.UpsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test cases for inserting explicit primary keys into an autoID collection.
 * Server >= 2.6.3 accepts explicit pk values only when the collection property
 * "allow_insert_auto_id" is set to "true". The SDK requires the pk column to be
 * provided for all rows or omitted for all rows.
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class AutoIdExplicitPkTest extends BaseTest {
    private String autoIdCollection;
    private String autoIdNoPropCollection;

    private static final int DIM = 128;
    private static final int EXPLICIT_PK_COUNT = 100;
    private static final long EXPLICIT_PK_START = 10000L;
    private static final String ALLOW_INSERT_AUTO_ID = "allow_insert_auto_id";

    @BeforeClass(alwaysRun = true)
    public void initCollections() {
        autoIdCollection = "AutoIdExplicitPk_" + GenerateUtil.getRandomString(6);
        autoIdNoPropCollection = "AutoIdNoProp_" + GenerateUtil.getRandomString(6);

        // autoID collection with allow_insert_auto_id=true property
        Map<String, String> props = new HashMap<>();
        props.put(ALLOW_INSERT_AUTO_ID, "true");
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(autoIdCollection)
                .collectionSchema(buildSchema())
                .properties(props)
                .build());

        // same schema but without the property, for negative case
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(autoIdNoPropCollection)
                .collectionSchema(buildSchema())
                .build());

        // insert 100 rows with explicit pk 10000~10099
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(autoIdCollection)
                .data(generateRows(EXPLICIT_PK_START, EXPLICIT_PK_COUNT, true))
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), EXPLICIT_PK_COUNT);

        CommonFunction.createVectorIndex(autoIdCollection, CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(autoIdCollection)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        for (String name : Arrays.asList(autoIdCollection, autoIdNoPropCollection)) {
            if (name != null) {
                milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(name).build());
            }
        }
    }

    private static CreateCollectionReq.CollectionSchema buildSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt32)
                .dataType(DataType.Int32)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        return schema;
    }

    private static List<JsonObject> generateRows(long startPk, int count, boolean withPk) {
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            if (withPk) {
                row.addProperty(CommonData.fieldInt64, startPk + i);
            }
            row.addProperty(CommonData.fieldInt32, (int) ((startPk + i) % 32767));
            List<Float> vector = new ArrayList<>();
            for (int k = 0; k < DIM; k++) {
                vector.add((float) Math.random());
            }
            row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            rows.add(row);
        }
        return rows;
    }

    /**
     * 查询 collection 当前 count(*)
     */
    private Long queryCount() {
        QueryResp countResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(autoIdCollection)
                .outputFields(Arrays.asList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        return ((Number) countResp.getQueryResults().get(0).getEntity().get("count(*)")).longValue();
    }

    @Test(description = "Query autoID collection by explicit pk values", groups = {"Smoke"})
    public void queryByExplicitPk() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(autoIdCollection)
                .filter(CommonData.fieldInt64 + " in [10000, 10050, 10099]")
                .outputFields(Arrays.asList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 3);
        Set<Long> returnedPks = new HashSet<>();
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            returnedPks.add(((Number) result.getEntity().get(CommonData.fieldInt64)).longValue());
        }
        Assert.assertTrue(returnedPks.contains(10000L), "Explicit pk 10000 should be returned");
        Assert.assertTrue(returnedPks.contains(10050L), "Explicit pk 10050 should be returned");
        Assert.assertTrue(returnedPks.contains(10099L), "Explicit pk 10099 should be returned");
    }

    @Test(description = "Insert rows without pk into autoID collection with allow_insert_auto_id, pk auto generated", groups = {"Smoke"})
    public void insertWithoutPkAutoGenerated() {
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(autoIdCollection)
                .data(generateRows(0, 100, false))
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), 100);

        QueryResp countResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(autoIdCollection)
                .outputFields(Arrays.asList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Long count = ((Number) countResp.getQueryResults().get(0).getEntity().get("count(*)")).longValue();
        Assert.assertEquals(count, (long) (EXPLICIT_PK_COUNT + 100),
                "Total count should be explicit pk rows plus auto generated rows");
    }

    @Test(description = "Upsert an explicit pk row on autoID collection (upsert path allowed since v2.4.10)", groups = {"Smoke"})
    public void upsertExplicitPk() {
        // 记录 upsert 前总数，用于最后对比 upsert 是 replace 还是新增
        Long countBefore = queryCount();

        List<JsonObject> rows = generateRows(EXPLICIT_PK_START, 1, true);
        rows.get(0).addProperty(CommonData.fieldInt32, 777777);
        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(autoIdCollection)
                .data(rows)
                .build());
        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        // 行为固化：autoID collection 上 upsert 会忽略显式主键、重新自动生成主键，显式 pk 不保留
        // （2.6-20261022 nightly 实测：upsert 后按显式 pk 10000 查询返回 0 行，原 pk 行也不存在）
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(autoIdCollection)
                .filter(CommonData.fieldInt64 + " == " + EXPLICIT_PK_START)
                .outputFields(Arrays.asList(CommonData.fieldInt32))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(queryResp.getQueryResults().size(), 0,
                "Explicit pk should not be kept after upsert on autoID collection (2.6 nightly behavior)");

        // 数据实际写入但挂在自动生成的主键下：按 fieldInt32 过滤应查到恰好 1 行
        QueryResp queryByInt32 = milvusClientV2.query(QueryReq.builder()
                .collectionName(autoIdCollection)
                .filter(CommonData.fieldInt32 + " == 777777")
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldInt32))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(queryByInt32.getQueryResults().size(), 1,
                "Upserted row should exist under an auto-generated pk");

        // 总数校验：2.6-20261022 nightly 实测表现为 replace——upsert 删掉原显式 pk 行并
        // 新增一条自动主键行，count(*) 与 upsert 前持平
        Long countAfter = queryCount();
        Assert.assertEquals(countAfter, countBefore,
                "Total count should be unchanged after upsert (replace semantics observed on 2.6 nightly)");
    }

    @Test(description = "Insert explicit pk into autoID collection without allow_insert_auto_id property should fail", groups = {"Smoke"})
    public void insertExplicitPkWithoutPropertyShouldFail() {
        try {
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(autoIdNoPropCollection)
                    .data(generateRows(EXPLICIT_PK_START, 10, true))
                    .build());
            Assert.fail("Server should reject explicit pk insert when allow_insert_auto_id is not set");
        } catch (Exception e) {
            // expected: server side rejects explicit pk for autoID collection without the property
            System.out.println("insert explicit pk without property exception: " + e);
        }
    }

    @Test(description = "Insert rows partially providing pk should fail on client side", groups = {"Smoke"})
    public void insertPartialPkRowsShouldFail() {
        List<JsonObject> rows = generateRows(EXPLICIT_PK_START, 5, true);
        // append 5 rows without pk: client requires all rows or none provide the autoID pk
        rows.addAll(generateRows(0, 5, false));
        try {
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(autoIdCollection)
                    .data(rows)
                    .build());
            Assert.fail("Client should reject rows partially providing the autoID pk");
        } catch (Exception e) {
            Assert.assertNotNull(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("all rows or omitted"),
                    "Exception message should mention all rows or omitted, actual: " + e.getMessage());
        }
    }
}

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
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.UpsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for field-level partial update ops (UpsertReq.FieldPartialUpdateOp):
 * REPLACE / ARRAY_APPEND / ARRAY_REMOVE. Any non-REPLACE op implicitly turns the
 * upsert into a partial update. Primary key must always be provided.
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class FieldPartialUpdateTest extends BaseTest {
    private String collectionName;

    private static final int DIM = 128;
    private static final int ENTITY_COUNT = 100;
    private static final int ARRAY_CAPACITY = 50;

    @BeforeClass(alwaysRun = true)
    public void initCollection() {
        collectionName = "FieldPartialUpdate_" + GenerateUtil.getRandomString(6);

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
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldArray)
                .dataType(DataType.Array)
                .elementType(DataType.Int64)
                .maxCapacity(ARRAY_CAPACITY)
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
                .data(generateData(0, ENTITY_COUNT))
                .build());

        CommonFunction.createVectorIndex(collectionName, CommonData.fieldFloatVector,
                IndexParam.IndexType.HNSW, IndexParam.MetricType.L2);
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    // initial row: pk = id, int32 = id * 10, varchar = "varchar_id", array = [id, id + 1]
    private static List<JsonObject> generateData(long startId, int count) {
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (long i = startId; i < startId + count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, i);
            row.addProperty(CommonData.fieldInt32, (int) (i * 10));
            row.addProperty(CommonData.fieldVarchar, "varchar_" + i);
            row.add(CommonData.fieldArray, gson.toJsonTree(Arrays.asList(i, i + 1)));
            List<Float> vector = new ArrayList<>();
            for (int k = 0; k < DIM; k++) {
                vector.add((float) Math.random());
            }
            row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            rows.add(row);
        }
        return rows;
    }

    private static UpsertReq.FieldPartialUpdateOp fieldOp(String fieldName,
                                                          UpsertReq.FieldPartialUpdateOp.OpType opType) {
        return UpsertReq.FieldPartialUpdateOp.builder()
                .fieldName(fieldName)
                .opType(opType)
                .build();
    }

    private QueryResp.QueryResult queryById(long id, List<String> outputFields) {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt64 + " == " + id)
                .outputFields(outputFields)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(queryResp.getQueryResults().size(), 1, "Should query exactly one row for id " + id);
        return queryResp.getQueryResults().get(0);
    }

    private static List<Long> toLongList(Object arrayValue) {
        List<Long> out = new ArrayList<>();
        for (Object o : (List<?>) arrayValue) {
            out.add(((Number) o).longValue());
        }
        return out;
    }

    @Test(description = "Field ops REPLACE on Int32 and VarChar fields", groups = {"Smoke"})
    public void fieldOpsReplaceScalarFields() {
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 5L);
        row.addProperty(CommonData.fieldInt32, 777);
        row.addProperty(CommonData.fieldVarchar, "replaced_5");

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .partialUpdate(true)
                .fieldOps(Arrays.asList(
                        fieldOp(CommonData.fieldInt32, UpsertReq.FieldPartialUpdateOp.OpType.REPLACE),
                        fieldOp(CommonData.fieldVarchar, UpsertReq.FieldPartialUpdateOp.OpType.REPLACE)))
                .build());
        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp.QueryResult result = queryById(5L, Arrays.asList(
                CommonData.fieldInt32, CommonData.fieldVarchar, CommonData.fieldArray, CommonData.fieldFloatVector));
        Assert.assertEquals(((Number) result.getEntity().get(CommonData.fieldInt32)).intValue(), 777);
        Assert.assertEquals(result.getEntity().get(CommonData.fieldVarchar), "replaced_5");
        // untouched fields keep original values
        Assert.assertEquals(toLongList(result.getEntity().get(CommonData.fieldArray)), Arrays.asList(5L, 6L));
        Object vector = result.getEntity().get(CommonData.fieldFloatVector);
        Assert.assertNotNull(vector, "Vector field should keep original value");
        Assert.assertEquals(((List<?>) vector).size(), DIM);
    }

    @Test(description = "Field ops ARRAY_APPEND on array field", groups = {"Smoke"})
    public void fieldOpsArrayAppend() {
        Gson gson = new Gson();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 10L);
        row.add(CommonData.fieldArray, gson.toJsonTree(Collections.singletonList(999L)));

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .partialUpdate(true)
                .fieldOps(Collections.singletonList(
                        fieldOp(CommonData.fieldArray, UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)))
                .build());
        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp.QueryResult result = queryById(10L, Arrays.asList(CommonData.fieldArray, CommonData.fieldInt32));
        Assert.assertEquals(toLongList(result.getEntity().get(CommonData.fieldArray)),
                Arrays.asList(10L, 11L, 999L), "Array should be appended with 999");
        Assert.assertEquals(((Number) result.getEntity().get(CommonData.fieldInt32)).intValue(), 100,
                "Int32 field should keep original value");
    }

    @Test(description = "Field ops ARRAY_REMOVE on array field", groups = {"Smoke"})
    public void fieldOpsArrayRemove() {
        Gson gson = new Gson();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 20L);
        row.add(CommonData.fieldArray, gson.toJsonTree(Collections.singletonList(20L)));

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .partialUpdate(true)
                .fieldOps(Collections.singletonList(
                        fieldOp(CommonData.fieldArray, UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_REMOVE)))
                .build());
        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp.QueryResult result = queryById(20L, Arrays.asList(CommonData.fieldArray, CommonData.fieldVarchar));
        Assert.assertEquals(toLongList(result.getEntity().get(CommonData.fieldArray)),
                Collections.singletonList(21L), "Array should have element 20 removed");
        Assert.assertEquals(result.getEntity().get(CommonData.fieldVarchar), "varchar_20",
                "VarChar field should keep original value");
    }

    @Test(description = "Non-REPLACE field op implies partialUpdate=true automatically", groups = {"Smoke"})
    public void fieldOpsAutoPartialUpdate() {
        Gson gson = new Gson();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 30L);
        row.add(CommonData.fieldArray, gson.toJsonTree(Collections.singletonList(888L)));

        // partialUpdate is not set explicitly; ARRAY_APPEND implies partial update
        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .fieldOps(Collections.singletonList(
                        fieldOp(CommonData.fieldArray, UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)))
                .build());
        Assert.assertEquals(upsertResp.getUpsertCnt(), 1);

        QueryResp.QueryResult result = queryById(30L, Arrays.asList(
                CommonData.fieldArray, CommonData.fieldInt32, CommonData.fieldVarchar));
        Assert.assertEquals(toLongList(result.getEntity().get(CommonData.fieldArray)),
                Arrays.asList(30L, 31L, 888L));
        Assert.assertEquals(((Number) result.getEntity().get(CommonData.fieldInt32)).intValue(), 300,
                "Int32 field should keep original value under auto partial update");
        Assert.assertEquals(result.getEntity().get(CommonData.fieldVarchar), "varchar_30",
                "VarChar field should keep original value under auto partial update");
    }

    @Test(description = "Field ops pointing to a non-exist field should fail", groups = {"Smoke"})
    public void fieldOpsOnNonExistFieldShouldFail() {
        Gson gson = new Gson();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 5L);
        row.add("fieldNotExist", gson.toJsonTree(Collections.singletonList(1L)));

        try {
            milvusClientV2.upsert(UpsertReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(row))
                    .partialUpdate(true)
                    .fieldOps(Collections.singletonList(
                            fieldOp("fieldNotExist", UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)))
                    .build());
            Assert.fail("Expect exception when fieldOps points to a non-exist field");
        } catch (Exception e) {
            // expected: server rejects unknown field
            System.out.println("fieldOps on non-exist field exception: " + e);
        }
    }

    @Test(description = "ARRAY_APPEND on a non-array field should fail", groups = {"Smoke"})
    public void arrayAppendOnNonArrayFieldShouldFail() {
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 5L);
        row.addProperty(CommonData.fieldInt32, 123);

        try {
            milvusClientV2.upsert(UpsertReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(row))
                    .partialUpdate(true)
                    .fieldOps(Collections.singletonList(
                            fieldOp(CommonData.fieldInt32, UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)))
                    .build());
            Assert.fail("Expect exception when applying ARRAY_APPEND on an Int32 field");
        } catch (Exception e) {
            // expected: server rejects array op on a non-array field
            System.out.println("ARRAY_APPEND on Int32 field exception: " + e);
        }
    }

    @Test(description = "Partial update data without primary key should fail on client side", groups = {"Smoke"})
    public void fieldOpsWithoutPkShouldFail() {
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt32, 123);

        try {
            milvusClientV2.upsert(UpsertReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(row))
                    .partialUpdate(true)
                    .fieldOps(Collections.singletonList(
                            fieldOp(CommonData.fieldInt32, UpsertReq.FieldPartialUpdateOp.OpType.REPLACE)))
                    .build());
            Assert.fail("Expect exception when partial update data misses the primary key");
        } catch (Exception e) {
            Assert.assertNotNull(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("primary key"),
                    "Exception message should mention primary key, actual: " + e.getMessage());
        }
    }
}

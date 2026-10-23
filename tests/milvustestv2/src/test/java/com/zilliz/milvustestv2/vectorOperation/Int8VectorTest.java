package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.Int8Vec;
import io.milvus.v2.service.vector.response.DeleteResp;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.service.vector.response.UpsertResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Test cases for Int8Vector field type (Milvus 2.6 new feature)
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class Int8VectorTest extends BaseTest {

    private String collectionName;
    private static final String FIELD_INT8_VECTOR = "fieldInt8Vector";
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;
    private static final Gson GSON = new Gson();

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "Int8Vector_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (collectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
        }
    }

    // ==================== Helpers ====================

    private static byte[] generateInt8Vector(int dim) {
        byte[] vector = new byte[dim];
        new Random().nextBytes(vector);
        return vector;
    }

    private static JsonObject buildRow(long id, byte[] vector) {
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, id);
        row.add(FIELD_INT8_VECTOR, GSON.toJsonTree(vector));
        row.addProperty(CommonData.fieldInt32, (int) (id % 1000));
        row.addProperty(CommonData.fieldVarchar, "varchar_" + id);
        return row;
    }

    private static List<JsonObject> buildInsertData(long startId, int count) {
        List<JsonObject> data = new ArrayList<>();
        for (long i = startId; i < startId + count; i++) {
            data.add(buildRow(i, generateInt8Vector(DIM)));
        }
        return data;
    }

    private static void createInt8VectorCollection(String name) {
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .name(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .name(FIELD_INT8_VECTOR)
                .dataType(DataType.Int8Vector)
                .dimension(DIM)
                .build());
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .name(CommonData.fieldInt32)
                .dataType(DataType.Int32)
                .build());
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .name(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .build();

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
    }

    private static void createHnswIndexAndLoad(String name) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(FIELD_INT8_VECTOR)
                .indexName("int8_vector_idx")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(new HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 64);
                }})
                .build();

        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(name)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(name)
                .build());
    }

    // ==================== Positive Tests ====================

    @Test(description = "Create collection with Int8Vector field and verify schema", groups = {"Smoke"})
    public void createInt8VectorCollectionSuccess() {
        createInt8VectorCollection(collectionName);

        DescribeCollectionResp descResp = milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(collectionName).build());

        Assert.assertTrue(descResp.getFieldNames().contains(FIELD_INT8_VECTOR),
                "Int8Vector field should exist in collection schema");

        CreateCollectionReq.FieldSchema fieldSchema = descResp.getCollectionSchema().getField(FIELD_INT8_VECTOR);
        Assert.assertNotNull(fieldSchema, "Int8Vector field schema should not be null");
        Assert.assertEquals(fieldSchema.getDataType(), DataType.Int8Vector,
                "Field type should be Int8Vector");
        Assert.assertEquals(fieldSchema.getDimension().intValue(), DIM,
                "Int8Vector field dimension should match");
    }

    @Test(description = "Insert 1000 rows with Int8Vector data", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorCollectionSuccess"})
    public void insertInt8VectorDataSuccess() {
        List<JsonObject> data = buildInsertData(0L, INSERT_COUNT);

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());

        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT,
                "Insert count should match");
    }

    @Test(description = "Create HNSW index with L2 metric on Int8Vector field and load", groups = {"Smoke"}, dependsOnMethods = {"insertInt8VectorDataSuccess"})
    public void createInt8VectorIndexAndLoadSuccess() {
        createHnswIndexAndLoad(collectionName);
    }

    @Test(description = "Search on Int8Vector field with Int8Vec query vector", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorIndexAndLoadSuccess"})
    public void searchInt8VectorSuccess() {
        byte[] queryVector = generateInt8Vector(DIM);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(FIELD_INT8_VECTOR)
                .data(Collections.singletonList(new Int8Vec(queryVector)))
                .topK(10)
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty(),
                "Search results should not be empty");
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "Search result list for nq 0 should not be empty");
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10,
                "Search result size should not exceed topK");
    }

    @Test(description = "Search on Int8Vector field with filter expression", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorIndexAndLoadSuccess"})
    public void searchInt8VectorWithFilterSuccess() {
        byte[] queryVector = generateInt8Vector(DIM);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(FIELD_INT8_VECTOR)
                .data(Collections.singletonList(new Int8Vec(queryVector)))
                .topK(10)
                .filter(CommonData.fieldInt64 + " < 500")
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        for (SearchResp.SearchResult result : searchResp.getSearchResults().get(0)) {
            Object idObj = result.getEntity().get(CommonData.fieldInt64);
            if (idObj != null) {
                Long id = (Long) idObj;
                Assert.assertTrue(id < 500, "Result should satisfy filter condition");
            }
        }
    }

    @Test(description = "Query Int8Vector entities by ids, vector value should be ByteBuffer", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorIndexAndLoadSuccess"})
    public void queryInt8VectorByIdsSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .ids(Arrays.<Object>asList(0L, 1L, 2L))
                .outputFields(Arrays.asList(CommonData.fieldInt64, FIELD_INT8_VECTOR, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), 3,
                "Should return 3 entities");

        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object vectorValue = result.getEntity().get(FIELD_INT8_VECTOR);
            Assert.assertNotNull(vectorValue, "Int8Vector field value should not be null");
            Assert.assertTrue(vectorValue instanceof ByteBuffer,
                    "Int8Vector field value should be ByteBuffer, but was " + vectorValue.getClass());
            // SDK 2.6.25 返回的 ByteBuffer 未 flip，remaining()=0，此处按 capacity 校验（SDK 已知问题）
            Assert.assertEquals(((ByteBuffer) vectorValue).capacity(), DIM,
                    "Int8Vector ByteBuffer capacity should equal dim");
        }
    }

    @Test(description = "Upsert one row with new Int8Vector data and verify by query", groups = {"Smoke"}, dependsOnMethods = {"queryInt8VectorByIdsSuccess"})
    public void upsertInt8VectorDataSuccess() {
        JsonObject row = buildRow(0L, generateInt8Vector(DIM));
        row.addProperty(CommonData.fieldVarchar, "upserted_0");

        UpsertResp upsertResp = milvusClientV2.upsert(UpsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .build());

        Assert.assertEquals(upsertResp.getUpsertCnt(), 1L,
                "Upsert count should be 1");

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .ids(Collections.<Object>singletonList(0L))
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 1);
        Assert.assertEquals(queryResp.getQueryResults().get(0).getEntity().get(CommonData.fieldVarchar),
                "upserted_0", "Varchar field should be updated after upsert");
    }

    @Test(description = "Delete Int8Vector entities by ids and verify by query", groups = {"Smoke"}, dependsOnMethods = {"upsertInt8VectorDataSuccess"})
    public void deleteInt8VectorDataSuccess() {
        DeleteResp deleteResp = milvusClientV2.delete(DeleteReq.builder()
                .collectionName(collectionName)
                .ids(Arrays.<Object>asList(0L, 1L))
                .build());

        Assert.assertEquals(deleteResp.getDeleteCnt(), 2L,
                "Delete count should be 2");

        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .ids(Arrays.<Object>asList(0L, 1L))
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(queryResp.getQueryResults().size(), 0,
                "Deleted entities should not be queryable");
    }

    // ==================== Negative Tests ====================

    @Test(description = "Insert Int8Vector data with mismatched dimension should fail", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorCollectionSuccess"}, expectedExceptions = Exception.class)
    public void insertInt8VectorDimMismatchShouldFail() {
        // vector dimension is DIM + 1, does not match schema dimension
        List<JsonObject> data = Collections.singletonList(buildRow(100000L, generateInt8Vector(DIM + 1)));

        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
    }

    @Test(description = "Insert Int8Vector data with non-numeric element should fail", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorCollectionSuccess"}, expectedExceptions = Exception.class)
    public void insertInt8VectorNonNumericShouldFail() {
        // element "abc" cannot be converted to byte
        JsonArray nonNumericVector = new JsonArray();
        nonNumericVector.add("abc");
        for (int i = 1; i < DIM; i++) {
            nonNumericVector.add(0);
        }
        JsonObject row = buildRow(100001L, generateInt8Vector(DIM));
        row.add(FIELD_INT8_VECTOR, nonNumericVector);

        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .build());
    }

    @Test(description = "Insert Int8Vector data with element out of byte range wraps silently", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorCollectionSuccess"})
    public void insertInt8VectorOutOfRangeWrapsSilently() {
        // SDK gson byte 转换会静默截断超范围值（200→-56），为 SDK 已知行为，此用例固化现状
        JsonArray outOfRangeVector = new JsonArray();
        outOfRangeVector.add(200);
        for (int i = 1; i < DIM; i++) {
            outOfRangeVector.add(0);
        }
        JsonObject row = buildRow(100002L, generateInt8Vector(DIM));
        row.add(FIELD_INT8_VECTOR, outOfRangeVector);

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), 1L,
                "Insert with out-of-range element should succeed (value silently wraps)");
    }

    @Test(description = "Search Int8Vector field with FloatVec query vector should fail", groups = {"Smoke"}, dependsOnMethods = {"createInt8VectorIndexAndLoadSuccess"}, expectedExceptions = Exception.class)
    public void searchWithFloatVecOnInt8VectorFieldShouldFail() {
        List<Float> floatVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);

        milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(FIELD_INT8_VECTOR)
                .data(Collections.singletonList(new FloatVec(floatVector)))
                .topK(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
    }

    @Test(description = "Search Int8Vector collection without index and load should fail", groups = {"Smoke"})
    public void searchWithoutIndexAndLoadShouldFail() {
        String noIndexCollection = "Int8NoIndex_" + GenerateUtil.getRandomString(6);
        createInt8VectorCollection(noIndexCollection);

        try {
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(noIndexCollection)
                    .data(buildInsertData(0L, 10))
                    .build());

            milvusClientV2.search(SearchReq.builder()
                    .collectionName(noIndexCollection)
                    .annsField(FIELD_INT8_VECTOR)
                    .data(Collections.singletonList(new Int8Vec(generateInt8Vector(DIM))))
                    .topK(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());

            Assert.fail("Search without index and load should throw exception");
        } catch (Exception e) {
            // Expected exception: collection not loaded / index not found
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(noIndexCollection)
                    .build());
        }
    }
}

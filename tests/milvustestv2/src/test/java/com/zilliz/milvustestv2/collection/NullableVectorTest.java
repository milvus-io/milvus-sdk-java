package com.zilliz.milvustestv2.collection;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddCollectionFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for nullable vector fields and nullable struct fields (Milvus 3.0 features)
 * Covers: nullable FloatVector insert/query/search, online add nullable vector field,
 * nullable Array-of-Struct, and constraint violations on struct sub-fields
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
public class NullableVectorTest extends BaseTest {

    private String nullableVectorCollectionName;
    private static final String fieldEmbedding = "embedding";
    private static final int DIM = CommonData.dim;
    private static final int INSERT_COUNT = 100;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        nullableVectorCollectionName = "NullableVector_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (nullableVectorCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(nullableVectorCollectionName)
                    .build());
        }
    }

    private static JsonArray floatVectorJson(int dim) {
        JsonArray floatVector = new JsonArray();
        for (Float v : GenerateUtil.generateFloatVector(1, 6, dim).get(0)) {
            floatVector.add(v);
        }
        return floatVector;
    }

    private static void createNullableVectorCollection(String collectionName) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(fieldEmbedding)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .isNullable(true)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
    }

    private static void indexAndLoad(String collectionName, String vectorField) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(vectorField)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
    }

    // ==================== Positive Tests ====================

    @Test(description = "Create collection with nullable FloatVector field", groups = {"Smoke", "V3"})
    public void createNullableVectorCollectionSuccess() {
        createNullableVectorCollection(nullableVectorCollectionName);
        Assert.assertTrue(milvusClientV2.listCollections().getCollectionNames().contains(nullableVectorCollectionName),
                "Collection with nullable vector field should be created");
    }

    @Test(description = "Insert 100 rows, even rows with vector and odd rows with null", groups = {"Smoke", "V3"}, dependsOnMethods = {"createNullableVectorCollectionSuccess"})
    public void insertNullableVectorDataSuccess() {
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < INSERT_COUNT; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            if (i % 2 == 0) {
                row.add(fieldEmbedding, floatVectorJson(DIM));
            } else {
                // insert null vector with JsonNull
                row.add(fieldEmbedding, JsonNull.INSTANCE);
            }
            dataList.add(row);
        }
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(nullableVectorCollectionName)
                .data(dataList)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT, "Insert count should match");
    }

    @Test(description = "Create index and load nullable vector collection", groups = {"Smoke", "V3"}, dependsOnMethods = {"insertNullableVectorDataSuccess"})
    public void createIndexAndLoad() {
        indexAndLoad(nullableVectorCollectionName, fieldEmbedding);
    }

    @Test(description = "Query nullable vector field, odd rows return null and even rows return vector", groups = {"Smoke", "V3"}, dependsOnMethods = {"createIndexAndLoad"})
    public void queryNullableVectorField() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(nullableVectorCollectionName)
                .filter(CommonData.fieldInt64 + " >= 0")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldEmbedding))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(queryResp.getQueryResults().size(), INSERT_COUNT);
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Long id = (Long) result.getEntity().get(CommonData.fieldInt64);
            Object vector = result.getEntity().get(fieldEmbedding);
            if (id % 2 == 0) {
                Assert.assertTrue(vector instanceof List,
                        "Even row " + id + " should have a non-null vector");
                Assert.assertEquals(((List<?>) vector).size(), DIM);
            } else {
                Assert.assertNull(vector, "Odd row " + id + " should read back null vector");
            }
        }
    }

    @Test(description = "Search on nullable vector field only returns rows with non-null vectors", groups = {"Smoke", "V3"}, dependsOnMethods = {"createIndexAndLoad"})
    public void searchNullableVectorOnlyReturnNonNull() {
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(nullableVectorCollectionName)
                .annsField(fieldEmbedding)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(20)
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(), "Search should return results");
        for (SearchResp.SearchResult result : searchResp.getSearchResults().get(0)) {
            Long id = (Long) result.getEntity().get(CommonData.fieldInt64);
            Assert.assertTrue(id % 2 == 0,
                    "Search should only return rows with non-null vectors, but got odd id: " + id);
        }
    }

    @Test(description = "Add nullable vector field online via addCollectionField, then insert and query", groups = {"Smoke", "V3"})
    public void addNullableVectorFieldOnline() {
        String collectionName = "AddNullableVec_" + GenerateUtil.getRandomString(6);
        try {
            // base collection with a normal (non-nullable) vector
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
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());

            // online add a nullable vector field
            milvusClientV2.addCollectionField(AddCollectionFieldReq.builder()
                    .collectionName(collectionName)
                    .fieldName("extra_embedding")
                    .dataType(DataType.FloatVector)
                    .dimension(64)
                    .isNullable(true)
                    .build());

            // insert data: even rows give the new vector, odd rows give null
            List<JsonObject> dataList = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                JsonObject row = new JsonObject();
                row.addProperty(CommonData.fieldInt64, (long) i);
                row.add(CommonData.fieldFloatVector, floatVectorJson(DIM));
                if (i % 2 == 0) {
                    row.add("extra_embedding", floatVectorJson(64));
                } else {
                    row.add("extra_embedding", JsonNull.INSTANCE);
                }
                dataList.add(row);
            }
            InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(dataList)
                    .build());
            Assert.assertEquals(insertResp.getInsertCnt(), 20);

            List<IndexParam> indexParams = new ArrayList<>();
            indexParams.add(IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.HNSW)
                    .metricType(IndexParam.MetricType.L2)
                    .build());
            indexParams.add(IndexParam.builder()
                    .fieldName("extra_embedding")
                    .indexType(IndexParam.IndexType.HNSW)
                    .metricType(IndexParam.MetricType.L2)
                    .build());
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(indexParams)
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                    .collectionName(collectionName)
                    .filter(CommonData.fieldInt64 + " >= 0")
                    .outputFields(Arrays.asList(CommonData.fieldInt64, "extra_embedding"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.assertEquals(queryResp.getQueryResults().size(), 20);
            for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
                Long id = (Long) result.getEntity().get(CommonData.fieldInt64);
                Object vector = result.getEntity().get("extra_embedding");
                if (id % 2 == 0) {
                    Assert.assertTrue(vector instanceof List, "Even row should have non-null extra vector");
                } else {
                    Assert.assertNull(vector, "Odd row should read back null extra vector");
                }
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Nullable struct field (Array of Struct), insert null for some rows and verify query", groups = {"Smoke", "V3"})
    public void nullableStructField() {
        String collectionName = "NullableStruct_" + GenerateUtil.getRandomString(6);
        try {
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldInt64)
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(false)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("nullable_struct")
                    .dataType(DataType.Array)
                    .elementType(DataType.Struct)
                    .maxCapacity(50)
                    .addStructField(AddFieldReq.builder()
                            .fieldName("sint")
                            .dataType(DataType.Int32)
                            .build())
                    .addStructField(AddFieldReq.builder()
                            .fieldName("svec")
                            .dataType(DataType.FloatVector)
                            .dimension(32)
                            .build())
                    .build());
            // make the whole struct field nullable
            schema.getStructField("nullable_struct").setNullable(true);

            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());

            // insert 20 rows, odd rows have null struct
            List<JsonObject> dataList = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                JsonObject row = new JsonObject();
                row.addProperty(CommonData.fieldInt64, (long) i);
                if (i % 2 == 0) {
                    JsonArray structArray = new JsonArray();
                    JsonObject struct = new JsonObject();
                    struct.addProperty("sint", i);
                    struct.add("svec", floatVectorJson(32));
                    structArray.add(struct);
                    row.add("nullable_struct", structArray);
                } else {
                    row.add("nullable_struct", JsonNull.INSTANCE);
                }
                dataList.add(row);
            }
            InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(dataList)
                    .build());
            Assert.assertEquals(insertResp.getInsertCnt(), 20);

            // index on struct vector sub-field is required before loading
            CommonFunction.createStructVectorIndex(collectionName, "nullable_struct", "svec",
                    "nullable_struct_svec_idx", IndexParam.MetricType.MAX_SIM_COSINE);
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                    .collectionName(collectionName)
                    .filter(CommonData.fieldInt64 + " >= 0")
                    .outputFields(Arrays.asList(CommonData.fieldInt64, "nullable_struct"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.assertEquals(queryResp.getQueryResults().size(), 20);
            for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
                Long id = (Long) result.getEntity().get(CommonData.fieldInt64);
                Object structData = result.getEntity().get("nullable_struct");
                if (id % 2 == 0) {
                    Assert.assertTrue(structData instanceof List,
                            "Even row " + id + " should have non-null struct");
                    Assert.assertEquals(((List<?>) structData).size(), 1);
                } else {
                    Assert.assertNull(structData, "Odd row " + id + " should read back null struct");
                }
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    // ==================== Negative Tests ====================

    @Test(description = "Struct sub-field with isNullable should fail (sub-fields do not support nullable/defaultValue/primaryKey)", groups = {"Smoke", "V3"})
    public void structSubFieldNullableShouldFail() {
        String collectionName = "StructSubNullable_" + GenerateUtil.getRandomString(6);
        try {
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldInt64)
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(false)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("bad_struct")
                    .dataType(DataType.Array)
                    .elementType(DataType.Struct)
                    .maxCapacity(10)
                    .addStructField(AddFieldReq.builder()
                            .fieldName("sub_field")
                            .dataType(DataType.Int32)
                            .isNullable(true)
                            .build())
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());
            Assert.fail("Struct sub-field with isNullable should throw exception");
        } catch (Exception e) {
            // expected: client ParamException or server rejection
            System.out.println("Expected exception for nullable struct sub-field: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Struct with JSON sub-field should fail", groups = {"Smoke", "V3"})
    public void structWithJsonSubFieldShouldFail() {
        String collectionName = "StructJsonSub_" + GenerateUtil.getRandomString(6);
        try {
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldInt64)
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(false)
                    .build());
            // JSON/Geometry/Timestamptz are not allowed as struct sub-fields
            schema.addField(AddFieldReq.builder()
                    .fieldName("json_struct")
                    .dataType(DataType.Array)
                    .elementType(DataType.Struct)
                    .maxCapacity(10)
                    .addStructField(AddFieldReq.builder()
                            .fieldName("json_sub")
                            .dataType(DataType.JSON)
                            .build())
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());
            Assert.fail("Struct with JSON sub-field should throw exception");
        } catch (Exception e) {
            // expected: client ParamException
            System.out.println("Expected exception for JSON struct sub-field: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "Array field with nullable, behavior recorded (Array itself does not support nullable)", groups = {"Smoke", "V3"})
    public void arrayFieldNullableBehavior() {
        String collectionName = "ArrayNullable_" + GenerateUtil.getRandomString(6);
        try {
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
            // Array field itself does not support nullable per Milvus 3.0 doc
            schema.addField(AddFieldReq.builder()
                    .fieldName(CommonData.fieldArray)
                    .dataType(DataType.Array)
                    .elementType(DataType.Int64)
                    .maxCapacity(100)
                    .isNullable(true)
                    .build());
            try {
                milvusClientV2.createCollection(CreateCollectionReq.builder()
                        .collectionName(collectionName)
                        .collectionSchema(schema)
                        .build());
                // behavior record: client javadoc says isNullable is only for scalar fields (not Array),
                // but the flag may be silently ignored or rejected by server
                System.out.println("Array field with isNullable(true) was ACCEPTED (behavior recorded)");
            } catch (Exception e) {
                // behavior record: rejected by client or server
                System.out.println("Array field with isNullable(true) was REJECTED: "
                        + e.getClass().getName() + ": " + e.getMessage());
            }
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }
}

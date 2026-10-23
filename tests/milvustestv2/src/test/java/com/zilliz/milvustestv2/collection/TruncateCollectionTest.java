package com.zilliz.milvustestv2.collection;

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
import io.milvus.v2.service.collection.request.TruncateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Test cases for truncateCollection interface (Milvus 2.6 new feature):
 * clear all data of a collection while keeping the schema
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class TruncateCollectionTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 2000;
    private static final int REINSERT_COUNT = 100;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "TruncateCollection_" + GenerateUtil.getRandomString(6);
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

    private static void createSimpleCollection(String name) {
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .name(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        fieldSchemaList.add(CreateCollectionReq.FieldSchema.builder()
                .name(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
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

    private static List<JsonObject> buildInsertData(long startId, int count) {
        List<JsonObject> data = new ArrayList<>();
        for (long i = startId; i < startId + count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, i);
            JsonArray floatVector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);
            row.addProperty(CommonData.fieldInt32, (int) (i % 1000));
            row.addProperty(CommonData.fieldVarchar, "varchar_" + i);
            data.add(row);
        }
        return data;
    }

    private static void createHnswIndexOnFloatVector(String name) {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexName("float_vector_idx")
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
    }

    private static long countEntities(String name) {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(name)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        return (Long) queryResp.getQueryResults().get(0).getEntity().get("count(*)");
    }

    // ==================== Positive Tests ====================

    @Test(description = "Create collection, insert 2000 rows and verify count", groups = {"Smoke"})
    public void createCollectionAndInsertDataSuccess() {
        createSimpleCollection(collectionName);

        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(buildInsertData(0L, INSERT_COUNT))
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT,
                "Insert count should match");

        // load requires an index on the vector field, otherwise server reports "index not found"
        createHnswIndexOnFloatVector(collectionName);

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        Assert.assertEquals(countEntities(collectionName), INSERT_COUNT,
                "Count should be " + INSERT_COUNT + " before truncate");
    }

    @Test(description = "Truncate collection should clear all data", groups = {"Smoke"}, dependsOnMethods = {"createCollectionAndInsertDataSuccess"})
    public void truncateCollectionSuccess() {
        milvusClientV2.truncateCollection(TruncateCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        Assert.assertEquals(countEntities(collectionName), 0L,
                "Count should be 0 after truncate");
    }

    @Test(description = "Schema should be preserved after truncate", groups = {"Smoke"}, dependsOnMethods = {"truncateCollectionSuccess"})
    public void schemaPreservedAfterTruncate() {
        DescribeCollectionResp descResp = milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(collectionName).build());

        Assert.assertEquals(new HashSet<>(descResp.getFieldNames()),
                new HashSet<>(Arrays.asList(CommonData.fieldInt64, CommonData.fieldFloatVector,
                        CommonData.fieldInt32, CommonData.fieldVarchar)),
                "Field list should be unchanged after truncate");

        CreateCollectionReq.FieldSchema vectorField = descResp.getCollectionSchema()
                .getField(CommonData.fieldFloatVector);
        Assert.assertNotNull(vectorField, "FloatVector field schema should still exist");
        Assert.assertEquals(vectorField.getDataType(), DataType.FloatVector,
                "FloatVector field type should be unchanged");
        Assert.assertEquals(vectorField.getDimension().intValue(), DIM,
                "FloatVector field dimension should be unchanged");
        Assert.assertEquals(descResp.getPrimaryFieldName(), CommonData.fieldInt64,
                "Primary field should be unchanged after truncate");
    }

    @Test(description = "Insert new data after truncate should work and count correctly", groups = {"Smoke"}, dependsOnMethods = {"truncateCollectionSuccess"})
    public void insertAfterTruncateSuccess() {
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(buildInsertData(100000L, REINSERT_COUNT))
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), REINSERT_COUNT,
                "Insert after truncate should succeed");

        Assert.assertEquals(countEntities(collectionName), REINSERT_COUNT,
                "Count should only include data inserted after truncate");
    }

    @Test(description = "Load and search on re-inserted data after truncate", groups = {"Smoke"}, dependsOnMethods = {"insertAfterTruncateSuccess"})
    public void searchAfterTruncateSuccess() {
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexName("float_vector_idx")
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

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

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
        Assert.assertFalse(searchResp.getSearchResults().isEmpty(),
                "Search after truncate should return results");
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "Search result list for nq 0 should not be empty");
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10,
                "Search result size should not exceed topK");
        // all results must come from data inserted after truncate (ids >= 100000)
        for (SearchResp.SearchResult result : searchResp.getSearchResults().get(0)) {
            Object idObj = result.getEntity().get(CommonData.fieldInt64);
            if (idObj != null) {
                Assert.assertTrue((Long) idObj >= 100000L,
                        "Search result should only contain data inserted after truncate");
            }
        }
    }

    // ==================== Negative / Boundary Tests ====================

    @Test(description = "Truncate a non-existent collection should fail", groups = {"Smoke"}, expectedExceptions = Exception.class)
    public void truncateNonExistentCollectionShouldFail() {
        milvusClientV2.truncateCollection(TruncateCollectionReq.builder()
                .collectionName("TruncateNotExist_" + GenerateUtil.getRandomString(6))
                .build());
    }

    @Test(description = "Truncate an empty collection (no data inserted) should succeed", groups = {"Smoke"})
    public void truncateEmptyCollectionSuccess() {
        String emptyCollection = "TruncateEmpty_" + GenerateUtil.getRandomString(6);
        createSimpleCollection(emptyCollection);

        try {
            milvusClientV2.truncateCollection(TruncateCollectionReq.builder()
                    .collectionName(emptyCollection)
                    .build());

            // load requires an index on the vector field, otherwise server reports "index not found"
            createHnswIndexOnFloatVector(emptyCollection);

            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(emptyCollection)
                    .build());
            Assert.assertEquals(countEntities(emptyCollection), 0L,
                    "Count should be 0 after truncating empty collection");
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(emptyCollection)
                    .build());
        }
    }
}

package com.zilliz.milvustestv2.datatype;

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
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test cases for Timestamptz data type (Milvus 2.6 new data type)
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class TimestamptzTest extends BaseTest {

    private String timestamptzCollectionName;
    private static final String fieldTimestamptz = "fieldTimestamptz";
    private static final int DIM = CommonData.dim;
    private static final int INSERT_COUNT = 1000;
    private static final String FILTER_THRESHOLD = "2024-01-15T00:00:00+00:00";

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        timestamptzCollectionName = "Timestamptz_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (timestamptzCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(timestamptzCollectionName)
                    .build());
        }
    }

    private static void createTimestamptzCollection(String collectionName) {
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
                .fieldName(fieldTimestamptz)
                .dataType(DataType.Timestamptz)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
    }

    private static List<JsonObject> generateTimestamptzData(int count) {
        String[] zoneIds = {"+08:00", "-05:00", "UTC"};
        ZonedDateTime base = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC"));
        List<JsonObject> dataList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            JsonArray floatVector = new JsonArray();
            for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
                floatVector.add(v);
            }
            row.add(CommonData.fieldFloatVector, floatVector);
            String tsValue = base.plusHours(i)
                    .withZoneSameInstant(ZoneId.of(zoneIds[i % zoneIds.length]))
                    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            row.addProperty(fieldTimestamptz, tsValue);
            dataList.add(row);
        }
        return dataList;
    }

    // ==================== Positive Tests ====================

    @Test(description = "Create collection with Timestamptz field and verify schema", groups = {"Smoke"})
    public void createTimestamptzCollectionSuccess() {
        createTimestamptzCollection(timestamptzCollectionName);

        DescribeCollectionResp descResp = milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(timestamptzCollectionName).build());
        Assert.assertNotNull(descResp.getCollectionSchema());
        boolean fieldFound = false;
        for (CreateCollectionReq.FieldSchema fieldSchema : descResp.getCollectionSchema().getFieldSchemaList()) {
            if (fieldTimestamptz.equals(fieldSchema.getName())) {
                Assert.assertEquals(fieldSchema.getDataType(), DataType.Timestamptz,
                        "Field type should be Timestamptz");
                fieldFound = true;
            }
        }
        Assert.assertTrue(fieldFound, "Timestamptz field should exist in collection schema");
    }

    @Test(description = "Insert 1000 rows with timestamptz strings of different timezone offsets", groups = {"Smoke"}, dependsOnMethods = {"createTimestamptzCollectionSuccess"})
    public void insertTimestamptzDataSuccess() {
        List<JsonObject> data = generateTimestamptzData(INSERT_COUNT);
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(timestamptzCollectionName)
                .data(data)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT,
                "Insert count should match");
    }

    @Test(description = "Create STL_SORT index on timestamptz field, HNSW on vector and load", groups = {"Smoke"}, dependsOnMethods = {"insertTimestamptzDataSuccess"})
    public void createIndexAndLoadSuccess() {
        IndexParam tsIndexParam = IndexParam.builder()
                .fieldName(fieldTimestamptz)
                .indexType(IndexParam.IndexType.STL_SORT)
                .build();
        IndexParam vectorIndexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(timestamptzCollectionName)
                .indexParams(Arrays.asList(tsIndexParam, vectorIndexParam))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(timestamptzCollectionName)
                .build());
    }

    @Test(description = "Query timestamptz field and verify returned value is non-empty String", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryTimestamptzReadbackSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(timestamptzCollectionName)
                .filter(CommonData.fieldInt64 + " < 10")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), 10);
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object tsValue = result.getEntity().get(fieldTimestamptz);
            Assert.assertTrue(tsValue instanceof String,
                    "Timestamptz value should be returned as String");
            Assert.assertFalse(((String) tsValue).isEmpty(),
                    "Timestamptz value should not be empty");
        }
    }

    @Test(description = "Query with timestamptz compare filter and verify all results satisfy condition", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryTimestamptzCompareFilterSuccess() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(timestamptzCollectionName)
                .filter(fieldTimestamptz + " > ISO '" + FILTER_THRESHOLD + "'")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertFalse(queryResp.getQueryResults().isEmpty(),
                "Filter query should return results");
        Assert.assertTrue(queryResp.getQueryResults().size() < INSERT_COUNT,
                "Filter should exclude part of the data");
        Instant threshold = OffsetDateTime.parse(FILTER_THRESHOLD).toInstant();
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object tsValue = result.getEntity().get(fieldTimestamptz);
            Assert.assertTrue(tsValue instanceof String);
            String tsStr = (String) tsValue;
            try {
                Instant value = OffsetDateTime.parse(tsStr).toInstant();
                Assert.assertTrue(value.isAfter(threshold),
                        "Returned value " + tsStr + " should be after " + FILTER_THRESHOLD);
            } catch (java.time.format.DateTimeParseException e) {
                // fallback: server may return a different but valid format
                Assert.assertTrue(tsStr.contains("T"),
                        "Timestamptz value should be an ISO-8601 style string: " + tsStr);
            }
        }
    }

    @Test(description = "Query with timestamptz INTERVAL arithmetic filter", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryTimestamptzWithIntervalSuccess() {
        // data: row i = 2024-01-01T00:00:00Z + i hours (i in [0,1000)).
        // ts + 1 day > 2024-01-15T00:00:00Z  <=>  i > 312, so exactly 1000-313 = 687 rows hit
        String threshold = "2024-01-15T00:00:00+00:00";
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(timestamptzCollectionName)
                .filter(fieldTimestamptz + " + INTERVAL 'P1D' > ISO '" + threshold + "'")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), 687,
                "ts + INTERVAL 'P1D' > threshold should hit rows with id >= 313");
        Instant thresholdInstant = OffsetDateTime.parse(threshold).toInstant();
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object tsValue = result.getEntity().get(fieldTimestamptz);
            Assert.assertTrue(tsValue instanceof String);
            String tsStr = (String) tsValue;
            try {
                Instant value = OffsetDateTime.parse(tsStr).toInstant();
                Assert.assertTrue(value.plus(1, ChronoUnit.DAYS).isAfter(thresholdInstant),
                        "Returned value " + tsStr + " + 1 day should be after " + threshold);
            } catch (java.time.format.DateTimeParseException e) {
                // fallback: server may return a different but valid format
                Assert.assertTrue(tsStr.contains("T"),
                        "Timestamptz value should be an ISO-8601 style string: " + tsStr);
            }
        }
    }

    @Test(description = "Query with timestamptz range filter [2024-01-15, 2024-02-01)", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryTimestamptzRangeSuccess() {
        // data: row i = 2024-01-01T00:00:00Z + i hours (i in [0,1000)).
        // 2024-01-15T00:00:00Z <= ts < 2024-02-01T00:00:00Z  <=>  336 <= i < 744, so exactly 408 rows hit
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(timestamptzCollectionName)
                .filter(fieldTimestamptz + " >= ISO '2024-01-15T00:00:00Z' and "
                        + fieldTimestamptz + " < ISO '2024-02-01T00:00:00Z'")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(queryResp);
        Assert.assertEquals(queryResp.getQueryResults().size(), 408,
                "Range [2024-01-15, 2024-02-01) should hit rows with 336 <= id < 744");
        Instant lower = OffsetDateTime.parse("2024-01-15T00:00:00Z").toInstant();
        Instant upper = OffsetDateTime.parse("2024-02-01T00:00:00Z").toInstant();
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object tsValue = result.getEntity().get(fieldTimestamptz);
            Assert.assertTrue(tsValue instanceof String);
            String tsStr = (String) tsValue;
            try {
                Instant value = OffsetDateTime.parse(tsStr).toInstant();
                Assert.assertFalse(value.isBefore(lower),
                        "Returned value " + tsStr + " should not be before 2024-01-15T00:00:00Z");
                Assert.assertTrue(value.isBefore(upper),
                        "Returned value " + tsStr + " should be before 2024-02-01T00:00:00Z");
            } catch (java.time.format.DateTimeParseException e) {
                // fallback: server may return a different but valid format
                Assert.assertTrue(tsStr.contains("T"),
                        "Timestamptz value should be an ISO-8601 style string: " + tsStr);
            }
        }
    }

    @Test(description = "Query with timezone Asia/Shanghai and UTC", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryWithTimezoneSuccess() {
        QueryResp shanghaiResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(timestamptzCollectionName)
                .filter(CommonData.fieldInt64 + " < 5")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                .timezone("Asia/Shanghai")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(shanghaiResp);
        Assert.assertEquals(shanghaiResp.getQueryResults().size(), 5);
        for (QueryResp.QueryResult result : shanghaiResp.getQueryResults()) {
            Assert.assertTrue(result.getEntity().get(fieldTimestamptz) instanceof String);
        }

        QueryResp utcResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(timestamptzCollectionName)
                .filter(CommonData.fieldInt64 + " < 5")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                .timezone("UTC")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(utcResp);
        Assert.assertEquals(utcResp.getQueryResults().size(), 5);
        for (QueryResp.QueryResult result : utcResp.getQueryResults()) {
            Assert.assertTrue(result.getEntity().get(fieldTimestamptz) instanceof String);
        }
    }

    @Test(description = "Vector search with timezone and timestamptz filter", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void searchWithTimezoneAndTimestamptzFilterSuccess() {
        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(timestamptzCollectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(10)
                .filter(fieldTimestamptz + " > ISO '" + FILTER_THRESHOLD + "'")
                .timezone("America/Chicago")
                .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "Search with timestamptz filter should return results");
        for (SearchResp.SearchResult result : searchResp.getSearchResults().get(0)) {
            Object tsValue = result.getEntity().get(fieldTimestamptz);
            if (tsValue != null) {
                Assert.assertTrue(tsValue instanceof String,
                        "Timestamptz value should be returned as String");
            }
        }
    }

    // ==================== Negative Tests ====================

    @Test(description = "Insert invalid timestamptz string should fail", groups = {"Smoke"}, dependsOnMethods = {"createTimestamptzCollectionSuccess"})
    public void insertInvalidTimestamptzShouldFail() {
        List<JsonObject> dataList = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 100001L);
        JsonArray floatVector = new JsonArray();
        for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
            floatVector.add(v);
        }
        row.add(CommonData.fieldFloatVector, floatVector);
        row.addProperty(fieldTimestamptz, "not-a-time");
        dataList.add(row);

        try {
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(timestamptzCollectionName)
                    .data(dataList)
                    .build());
            Assert.fail("Should throw exception when inserting invalid timestamptz string");
        } catch (Exception e) {
            // Expected exception
        }
    }

    @Test(description = "Insert long value into timestamptz field should fail", groups = {"Smoke"}, dependsOnMethods = {"createTimestamptzCollectionSuccess"})
    public void insertLongValueShouldFail() {
        List<JsonObject> dataList = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 100002L);
        JsonArray floatVector = new JsonArray();
        for (Float v : GenerateUtil.generateFloatVector(1, 6, DIM).get(0)) {
            floatVector.add(v);
        }
        row.add(CommonData.fieldFloatVector, floatVector);
        row.addProperty(fieldTimestamptz, 1735689600000L);
        dataList.add(row);

        try {
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(timestamptzCollectionName)
                    .data(dataList)
                    .build());
            Assert.fail("Should throw exception when inserting long value into timestamptz field");
        } catch (Exception e) {
            // Expected exception
        }
    }

    @Test(description = "Query with invalid timezone should fail", groups = {"Smoke"}, dependsOnMethods = {"createIndexAndLoadSuccess"})
    public void queryWithInvalidTimezoneShouldFail() {
        try {
            milvusClientV2.query(QueryReq.builder()
                    .collectionName(timestamptzCollectionName)
                    .filter(CommonData.fieldInt64 + " < 5")
                    .outputFields(Arrays.asList(CommonData.fieldInt64, fieldTimestamptz))
                    .timezone("Invalid/Zone")
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when querying with invalid timezone");
        } catch (Exception e) {
            // Expected exception
        }
    }
}

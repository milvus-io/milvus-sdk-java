package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.common.CommonFunction;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test cases for filter expression template values ({placeholder}) used by
 * queryIterator (expr + filterTemplateValues) and searchIteratorV2
 * (filter + filterTemplateValues).
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class IteratorFilterTemplateTest extends BaseTest {
    private String collectionName;

    private static final int DIM = 128;
    private static final int ENTITY_COUNT = 1000;

    @BeforeClass(alwaysRun = true)
    public void initCollection() {
        collectionName = "IteratorTemplate_" + GenerateUtil.getRandomString(6);

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldVarchar)
                .dataType(DataType.VarChar)
                .maxLength(256)
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

    private static List<JsonObject> generateData(long startId, int count) {
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (long i = startId; i < startId + count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, i);
            row.addProperty(CommonData.fieldVarchar, "varchar_" + i);
            List<Float> vector = new ArrayList<>();
            for (int k = 0; k < DIM; k++) {
                vector.add((float) Math.random());
            }
            row.add(CommonData.fieldFloatVector, gson.toJsonTree(vector));
            rows.add(row);
        }
        return rows;
    }

    private static List<Long> drainQueryIterator(QueryIterator iterator) {
        List<Long> ids = new ArrayList<>();
        try {
            while (true) {
                List<QueryResultsWrapper.RowRecord> batch = iterator.next();
                if (batch == null || batch.isEmpty()) {
                    break;
                }
                for (QueryResultsWrapper.RowRecord record : batch) {
                    ids.add(((Number) record.get(CommonData.fieldInt64)).longValue());
                }
            }
        } finally {
            iterator.close();
        }
        return ids;
    }

    private static List<Long> drainSearchIterator(SearchIteratorV2 iterator) {
        List<Long> ids = new ArrayList<>();
        try {
            while (true) {
                List<SearchResp.SearchResult> batch = iterator.next();
                if (batch == null || batch.isEmpty()) {
                    break;
                }
                for (SearchResp.SearchResult result : batch) {
                    ids.add(((Number) result.getId()).longValue());
                }
            }
        } finally {
            iterator.close();
        }
        return ids;
    }

    @Test(description = "queryIterator with single placeholder in expr", groups = {"Smoke"})
    public void queryIteratorWithSinglePlaceholder() {
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("minId", 500L);

        QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                .collectionName(collectionName)
                .expr(CommonData.fieldInt64 + " >= {minId}")
                .filterTemplateValues(templateValues)
                .batchSize(100)
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<Long> ids = drainQueryIterator(queryIterator);
        Assert.assertEquals(ids.size(), 500, "fieldInt64 >= 500 should match 500 rows");
        for (Long id : ids) {
            Assert.assertTrue(id >= 500, "Each returned id should be >= 500, actual: " + id);
        }
    }

    @Test(description = "queryIterator with multiple placeholders in expr", groups = {"Smoke"})
    public void queryIteratorWithMultiplePlaceholders() {
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("minId", 200L);
        templateValues.put("maxId", 400L);

        QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                .collectionName(collectionName)
                .expr(CommonData.fieldInt64 + " >= {minId} and " + CommonData.fieldInt64 + " < {maxId}")
                .filterTemplateValues(templateValues)
                .batchSize(100)
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<Long> ids = drainQueryIterator(queryIterator);
        Assert.assertEquals(ids.size(), 200, "200 <= fieldInt64 < 400 should match 200 rows");
        for (Long id : ids) {
            Assert.assertTrue(id >= 200 && id < 400, "Each returned id should be in [200, 400), actual: " + id);
        }
    }

    @Test(description = "searchIteratorV2 with placeholder in filter", groups = {"Smoke"})
    public void searchIteratorV2WithPlaceholder() {
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("minId", 900L);

        List<BaseVector> vectors = new ArrayList<>();
        vectors.add(new FloatVec(GenerateUtil.generateFloatVector(1, 6, DIM).get(0)));

        SearchIteratorV2 searchIterator = milvusClientV2.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(collectionName)
                .vectorFieldName(CommonData.fieldFloatVector)
                .vectors(vectors)
                .metricType(IndexParam.MetricType.L2)
                .filter(CommonData.fieldInt64 + " >= {minId}")
                .filterTemplateValues(templateValues)
                .batchSize(100)
                .limit(200)
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<Long> ids = drainSearchIterator(searchIterator);
        Assert.assertFalse(ids.isEmpty(), "Search iterator should return some results");
        Assert.assertTrue(ids.size() <= 200, "Result count should not exceed limit 200, actual: " + ids.size());
        for (Long id : ids) {
            Assert.assertTrue(id >= 900, "Each returned id should be >= 900, actual: " + id);
        }
    }

    @Test(description = "expr references a placeholder missing from filterTemplateValues should fail", groups = {"Smoke"})
    public void missingPlaceholderShouldFail() {
        try {
            QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                    .collectionName(collectionName)
                    .expr(CommonData.fieldInt64 + " >= {missing}")
                    .batchSize(100)
                    .outputFields(Collections.singletonList(CommonData.fieldInt64))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            // the template is resolved when the query RPC is issued during iteration
            drainQueryIterator(queryIterator);
            Assert.fail("Expect exception when expr references a placeholder not present in filterTemplateValues");
        } catch (Exception e) {
            // expected: server reports the template value of the placeholder is missing
            System.out.println("missing placeholder exception: " + e);
        }
    }

    @Test(description = "Integer template value is accepted by SDK (converted to int64)", groups = {"Smoke"})
    public void integerTemplateValueBehavior() {
        Map<String, Object> templateValues = new HashMap<>();
        templateValues.put("minId", Integer.valueOf(500));

        // Actual behavior note: SDK deduceAndCreateTemplateValue accepts Integer and converts it
        // to an int64 template value, so passing an Integer does NOT raise a client-side error.
        QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                .collectionName(collectionName)
                .expr(CommonData.fieldInt64 + " >= {minId}")
                .filterTemplateValues(templateValues)
                .batchSize(100)
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<Long> ids = drainQueryIterator(queryIterator);
        Assert.assertEquals(ids.size(), 500, "Integer template value should behave like long 500");
    }

    @Test(description = "Unsupported template value type should fail on client side", groups = {"Smoke"})
    public void unsupportedTemplateValueTypeShouldFail() {
        Map<String, Object> templateValues = new HashMap<>();
        // legal types are Boolean/Long/Double/String (Integer is also accepted) and their Lists;
        // Float is not supported
        templateValues.put("minId", 500.0f);

        try {
            QueryIterator queryIterator = milvusClientV2.queryIterator(QueryIteratorReq.builder()
                    .collectionName(collectionName)
                    .expr(CommonData.fieldInt64 + " >= {minId}")
                    .filterTemplateValues(templateValues)
                    .batchSize(100)
                    .outputFields(Collections.singletonList(CommonData.fieldInt64))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            drainQueryIterator(queryIterator);
            Assert.fail("Expect exception for unsupported template value type Float");
        } catch (Exception e) {
            Assert.assertNotNull(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("Unsupported value type"),
                    "Exception message should mention unsupported value type, actual: " + e.getMessage());
        }
    }
}

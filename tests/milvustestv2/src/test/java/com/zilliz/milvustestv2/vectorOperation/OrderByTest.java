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
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.OrderByField;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Test cases for Milvus 3.0 order_by_fields of query/search.
 * Data: 1000 rows, fieldInt64=fieldInt32=i, fieldDouble=i*0.5, fieldVarchar="brand_"+(i%5).
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class OrderByTest extends BaseTest {

    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;

    private String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "OrderBy_" + GenerateUtil.getRandomString(6);

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

    private QueryResp queryWithOrderBy(String filter, List<OrderByField> orderByFields, long limit) {
        return milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(filter)
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldInt32, CommonData.fieldVarchar))
                .orderByFields(orderByFields)
                .limit(limit)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
    }

    // ==================== Positive Cases ====================

    @Test(description = "Query with orderByFields(fieldInt32 ASC), results should be ascending",
            groups = {"Smoke", "V3"})
    public void queryOrderByInt32Asc() {
        QueryResp queryResp = queryWithOrderBy(CommonData.fieldInt64 + " >= 0",
                Collections.singletonList(OrderByField.builder()
                        .fieldName(CommonData.fieldInt32)
                        .direction(AggDirection.ASC)
                        .build()), 50);

        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        Assert.assertEquals(results.size(), 50);
        for (int i = 1; i < results.size(); i++) {
            int prev = ((Number) results.get(i - 1).getEntity().get(CommonData.fieldInt32)).intValue();
            int curr = ((Number) results.get(i).getEntity().get(CommonData.fieldInt32)).intValue();
            Assert.assertTrue(prev <= curr, "Results should be ordered by fieldInt32 ASC: " + prev + " > " + curr);
        }
    }

    @Test(description = "Query with multi-field orderBy(fieldVarchar ASC, fieldInt32 DESC)",
            groups = {"Smoke", "V3"})
    public void queryOrderByMultiFields() {
        QueryResp queryResp = queryWithOrderBy(CommonData.fieldInt64 + " >= 0",
                Arrays.asList(
                        OrderByField.builder().fieldName(CommonData.fieldVarchar).direction(AggDirection.ASC).build(),
                        OrderByField.builder().fieldName(CommonData.fieldInt32).direction(AggDirection.DESC).build()),
                50);

        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        Assert.assertEquals(results.size(), 50);
        for (int i = 1; i < results.size(); i++) {
            String prevBrand = String.valueOf(results.get(i - 1).getEntity().get(CommonData.fieldVarchar));
            String currBrand = String.valueOf(results.get(i).getEntity().get(CommonData.fieldVarchar));
            int cmp = prevBrand.compareTo(currBrand);
            Assert.assertTrue(cmp <= 0, "Results should be ordered by fieldVarchar ASC");
            if (cmp == 0) {
                int prevInt = ((Number) results.get(i - 1).getEntity().get(CommonData.fieldInt32)).intValue();
                int currInt = ((Number) results.get(i).getEntity().get(CommonData.fieldInt32)).intValue();
                Assert.assertTrue(prevInt >= currInt,
                        "Within the same brand, results should be ordered by fieldInt32 DESC");
            }
        }
    }

    @Test(description = "Search with orderByFields(fieldInt32 DESC) and limit(20), results should be descending",
            groups = {"Smoke", "V3"})
    public void searchOrderByInt32Desc() {
        List<BaseVector> data = Collections.singletonList(
                new FloatVec(GenerateUtil.generateFloatVector(1, 6, DIM).get(0)));
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(data)
                .limit(20)
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldInt32))
                .orderByFields(Collections.singletonList(OrderByField.builder()
                        .fieldName(CommonData.fieldInt32)
                        .direction(AggDirection.DESC)
                        .build()))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(searchResp.getSearchResults().size(), 1);
        List<SearchResp.SearchResult> results = searchResp.getSearchResults().get(0);
        Assert.assertEquals(results.size(), 20);
        for (int i = 1; i < results.size(); i++) {
            int prev = ((Number) results.get(i - 1).getEntity().get(CommonData.fieldInt32)).intValue();
            int curr = ((Number) results.get(i).getEntity().get(CommonData.fieldInt32)).intValue();
            Assert.assertTrue(prev >= curr, "Search results should be ordered by fieldInt32 DESC: " + prev + " < " + curr);
        }
    }

    @Test(description = "Query combining filter and orderByFields(fieldInt32 DESC)",
            groups = {"Smoke", "V3"})
    public void queryOrderByWithFilter() {
        QueryResp queryResp = queryWithOrderBy(
                CommonData.fieldInt32 + " >= 100 and " + CommonData.fieldInt32 + " < 200",
                Collections.singletonList(OrderByField.builder()
                        .fieldName(CommonData.fieldInt32)
                        .direction(AggDirection.DESC)
                        .build()), 20);

        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        Assert.assertEquals(results.size(), 20);
        for (int i = 0; i < results.size(); i++) {
            int value = ((Number) results.get(i).getEntity().get(CommonData.fieldInt32)).intValue();
            Assert.assertTrue(value >= 100 && value < 200, "Filter not satisfied: " + value);
            if (i > 0) {
                int prev = ((Number) results.get(i - 1).getEntity().get(CommonData.fieldInt32)).intValue();
                Assert.assertTrue(prev >= value, "Results should be ordered by fieldInt32 DESC");
            }
        }
        // DESC on [100, 200) with limit 20 should start from 199
        int first = ((Number) results.get(0).getEntity().get(CommonData.fieldInt32)).intValue();
        Assert.assertEquals(first, 199);
    }

    // ==================== Negative Cases ====================

    @Test(description = "OrderByField with empty fieldName should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void orderByEmptyFieldNameShouldFail() {
        OrderByField.builder()
                .fieldName("")
                .direction(AggDirection.ASC)
                .build();
    }

    @Test(description = "OrderByField with explicit null direction should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void orderByNullDirectionShouldFail() {
        OrderByField.builder()
                .fieldName(CommonData.fieldInt32)
                .direction(null)
                .build();
    }

    @Test(description = "Order by a field that does not exist, record the actual behavior",
            groups = {"Smoke", "V3"})
    public void orderByFieldNotExist() {
        try {
            QueryResp queryResp = queryWithOrderBy(CommonData.fieldInt64 + " >= 0",
                    Collections.singletonList(OrderByField.builder()
                            .fieldName("fieldNotExist")
                            .direction(AggDirection.ASC)
                            .build()), 10);
            // Since 3.0.5 the client does not validate the field name; the server may report
            // an error or ignore the unknown field. If no exception was raised, log the response.
            log.warn("Order by a non-existent field did not fail, result size: {}",
                    queryResp.getQueryResults().size());
        } catch (Exception e) {
            log.info("Order by a non-existent field failed on the server as expected: {}", e.getMessage());
        }
    }
}

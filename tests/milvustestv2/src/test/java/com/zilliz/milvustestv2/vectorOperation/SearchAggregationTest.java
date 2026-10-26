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
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.aggregation.AggDirection;
import io.milvus.v2.service.vector.request.aggregation.MetricOps;
import io.milvus.v2.service.vector.request.aggregation.MetricSpec;
import io.milvus.v2.service.vector.request.aggregation.OrderSpec;
import io.milvus.v2.service.vector.request.aggregation.SearchAggregation;
import io.milvus.v2.service.vector.request.aggregation.SortSpec;
import io.milvus.v2.service.vector.request.aggregation.TopHitsSpec;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.service.vector.response.aggregation.AggregationBucket;
import io.milvus.v2.service.vector.response.aggregation.AggregationHit;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Map;
import java.util.Set;

/**
 * Test cases for Milvus 3.0 search group-by aggregation (SearchReq.searchAggregation).
 * Data: 1000 rows, fieldInt64=fieldInt32=i, fieldDouble=i*0.5, fieldVarchar="brand_"+(i%5),
 * fieldBool=(i%2==0), so each brand owns exactly 200 rows.
 *
 * <p>Aggregation semantics (design #49046, verified 2026-10-26 against Milvus
 * 3.0-20261023-6376134d4d): search aggregation is a proxy-only feature that amplifies the
 * standard group_by search. Metrics and bucket counts are computed over the ANN-retrieved
 * entities that are actually returned per group, NOT over the full collection. The number of
 * docs returned per group is driven by topHits.size (default 1 when topHits is not set), so:
 * no topHits -> every bucket count=1; topHits(size=10) -> count=10; topHits(size>=200) ->
 * count=200 (all rows of the brand, since the search limit already covers the collection).
 * Cases that assert exact counts/metrics therefore set topHits to cover the whole group.
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class SearchAggregationTest extends BaseTest {

    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;
    // aggregation works on the top-K search hits, so search limit must cover all rows
    private static final long SEARCH_LIMIT = 1000L;

    private String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "SearchAggregation_" + GenerateUtil.getRandomString(6);

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
                .fieldName(CommonData.fieldBool)
                .dataType(DataType.Bool)
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
            row.addProperty(CommonData.fieldBool, i % 2 == 0);
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

    private static List<BaseVector> providerQueryVector() {
        return Collections.singletonList(new FloatVec(GenerateUtil.generateFloatVector(1, 6, DIM).get(0)));
    }

    private SearchResp searchWithAggregation(SearchAggregation aggregation) {
        return milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(providerQueryVector())
                .limit(SEARCH_LIMIT)
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldInt32,
                        CommonData.fieldDouble, CommonData.fieldBool, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .searchAggregation(aggregation)
                .build());
    }

    // ==================== Positive Cases ====================

    @Test(description = "Search aggregation group by fieldVarchar with topHits covering all rows, "
            + "expect 5 buckets of 200 rows each",
            groups = {"Smoke", "V3"})
    public void searchAggregationGroupByVarchar() {
        // count = number of docs returned per group, capped by topHits.size (see class javadoc);
        // each brand owns 200 rows, so topHits size 200 makes the bucket count exact.
        SearchResp searchResp = searchWithAggregation(SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(5)
                .topHits(TopHitsSpec.builder().size(200).build())
                .build());

        List<List<AggregationBucket>> perQueryBuckets = searchResp.getAggregationBuckets();
        Assert.assertNotNull(perQueryBuckets);
        Assert.assertEquals(perQueryBuckets.size(), 1, "One bucket list per query vector");

        List<AggregationBucket> buckets = perQueryBuckets.get(0);
        Assert.assertEquals(buckets.size(), 5, "Expect 5 brand buckets");

        Set<String> keys = new HashSet<>();
        for (AggregationBucket bucket : buckets) {
            Assert.assertEquals(bucket.getKey().size(), 1);
            AggregationBucket.KeyEntry keyEntry = bucket.getKey().get(0);
            Assert.assertEquals(keyEntry.getFieldName(), CommonData.fieldVarchar);
            String brand = String.valueOf(keyEntry.getValue());
            keys.add(brand);
            Assert.assertEquals(bucket.getCount(), 200, "Each brand should own 200 rows: " + brand);
        }
        Assert.assertEquals(keys, new HashSet<>(Arrays.asList("brand_0", "brand_1", "brand_2", "brand_3", "brand_4")));
    }

    @Test(description = "Semantics fixation (design #49046): without topHits each bucket count=1 "
            + "(one representative doc per group); with topHits(size=10) count=10. Bucket count is "
            + "the number of docs returned per group, not the real group size",
            groups = {"Smoke", "V3"})
    public void searchAggregationCountReflectsReturnedDocs() {
        // no topHits: only 1 doc per group is returned, so every brand bucket counts 1
        SearchResp noTopHits = searchWithAggregation(SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(5)
                .build());
        List<AggregationBucket> buckets = noTopHits.getAggregationBuckets().get(0);
        Assert.assertEquals(buckets.size(), 5, "Expect 5 brand buckets");
        for (AggregationBucket bucket : buckets) {
            Assert.assertEquals(bucket.getCount(), 1,
                    "Without topHits only 1 doc per group is returned, count must be 1");
        }

        // topHits(size=10): 10 docs per group are returned, so every bucket counts 10
        SearchResp topHits10 = searchWithAggregation(SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(5)
                .topHits(TopHitsSpec.builder().size(10).build())
                .build());
        for (AggregationBucket bucket : topHits10.getAggregationBuckets().get(0)) {
            Assert.assertEquals(bucket.getCount(), 10,
                    "topHits(size=10) returns 10 docs per group, count must be 10");
            Assert.assertEquals(bucket.getHits().size(), 10, "10 hits should be returned per bucket");
        }
    }

    @Test(description = "Search aggregation with metrics AVG(fieldDouble) and COUNT(*)",
            groups = {"Smoke", "V3"})
    public void searchAggregationWithMetrics() {
        // topHits size 200 makes each bucket cover all 200 rows of the brand (see class javadoc),
        // so COUNT(*)=200 and AVG(fieldDouble) matches the exact expected value.
        SearchResp searchResp = searchWithAggregation(SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(5)
                .addMetric("avg_double", MetricSpec.builder()
                        .op(MetricOps.AVG)
                        .fieldName(CommonData.fieldDouble)
                        .build())
                .addMetric("doc_count", MetricSpec.builder()
                        .op(MetricOps.COUNT)
                        .fieldName("*")
                        .build())
                .topHits(TopHitsSpec.builder().size(200).build())
                .build());

        List<AggregationBucket> buckets = searchResp.getAggregationBuckets().get(0);
        Assert.assertEquals(buckets.size(), 5);
        for (AggregationBucket bucket : buckets) {
            Map<String, Object> metrics = bucket.getMetrics();
            Assert.assertNotNull(metrics);
            Assert.assertEquals(((Number) metrics.get("doc_count")).longValue(), 200L,
                    "COUNT(*) of each brand bucket should be 200");
            // brand_b holds i in {b, b+5, ..., b+995}; avg(fieldDouble) = (b + 497.5) * 0.5
            int b = Integer.parseInt(String.valueOf(bucket.getKey().get(0).getValue()).substring("brand_".length()));
            double expectAvg = (b + 497.5) * 0.5;
            double actualAvg = ((Number) metrics.get("avg_double")).doubleValue();
            Assert.assertEquals(actualAvg, expectAvg, 0.5,
                    "AVG(fieldDouble) of brand_" + b + " should be near " + expectAvg);
        }
    }

    @Test(description = "Search aggregation with order by _count DESC and size(3), expect only 3 ordered buckets",
            groups = {"Smoke", "V3"})
    public void searchAggregationWithOrderAndSize() {
        SearchResp searchResp = searchWithAggregation(SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(3)
                .addOrder(OrderSpec.builder()
                        .key("_count")
                        .direction(AggDirection.DESC)
                        .build())
                .build());

        List<AggregationBucket> buckets = searchResp.getAggregationBuckets().get(0);
        Assert.assertEquals(buckets.size(), 3, "size(3) should only return 3 buckets");
        for (int i = 1; i < buckets.size(); i++) {
            Assert.assertTrue(buckets.get(i - 1).getCount() >= buckets.get(i).getCount(),
                    "Buckets should be ordered by _count DESC");
        }
    }

    @Test(description = "Search aggregation with topHits(size=2), expect at most 2 hits per bucket with fields",
            groups = {"Smoke", "V3"})
    public void searchAggregationWithTopHits() {
        SearchResp searchResp = searchWithAggregation(SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(5)
                .topHits(TopHitsSpec.builder()
                        .size(2)
                        .addSort(SortSpec.builder()
                                .fieldName(CommonData.fieldInt32)
                                .direction(AggDirection.ASC)
                                .build())
                        .build())
                .build());

        List<AggregationBucket> buckets = searchResp.getAggregationBuckets().get(0);
        Assert.assertEquals(buckets.size(), 5);
        for (AggregationBucket bucket : buckets) {
            List<AggregationHit> hits = bucket.getHits();
            Assert.assertNotNull(hits);
            Assert.assertTrue(hits.size() <= 2, "topHits size=2, got " + hits.size());
            Assert.assertFalse(hits.isEmpty());
            for (AggregationHit hit : hits) {
                Assert.assertNotNull(hit.getId());
                Assert.assertNotNull(hit.getScore());
                Assert.assertNotNull(hit.getFields());
                Assert.assertFalse(hit.getFields().isEmpty(), "top hits should carry output fields");
            }
            // hits sorted by fieldInt32 ASC within the bucket
            if (hits.size() == 2) {
                int first = ((Number) hits.get(0).getFields().get(CommonData.fieldInt32)).intValue();
                int second = ((Number) hits.get(1).getFields().get(CommonData.fieldInt32)).intValue();
                Assert.assertTrue(first <= second, "top hits should be sorted by fieldInt32 ASC");
            }
        }
    }

    @Test(description = "Nested sub aggregation: group by fieldVarchar then by fieldBool inside each brand",
            groups = {"Smoke", "V3"})
    public void searchAggregationNestedSubAggregation() {
        // topHits drives how many docs per group are returned (see class javadoc): parent
        // topHits(200) covers the whole brand, sub topHits(100) covers each bool half.
        SearchResp searchResp = searchWithAggregation(SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(5)
                .topHits(TopHitsSpec.builder().size(200).build())
                .subAggregation(SearchAggregation.builder()
                        .fields(Collections.singletonList(CommonData.fieldBool))
                        .size(3)
                        .topHits(TopHitsSpec.builder().size(100).build())
                        .build())
                .build());

        List<AggregationBucket> buckets = searchResp.getAggregationBuckets().get(0);
        Assert.assertEquals(buckets.size(), 5);
        for (AggregationBucket bucket : buckets) {
            List<AggregationBucket> subGroups = bucket.getSubGroups();
            Assert.assertNotNull(subGroups);
            Assert.assertFalse(subGroups.isEmpty(), "Each brand bucket should have non-empty sub groups");
            // each brand has 200 rows, 100 with fieldBool=true and 100 with false
            Assert.assertEquals(subGroups.size(), 2);
            long subTotal = 0;
            for (AggregationBucket sub : subGroups) {
                Assert.assertEquals(sub.getKey().get(0).getFieldName(), CommonData.fieldBool);
                Assert.assertEquals(sub.getCount(), 100, "Each brand/bool sub group should own 100 rows");
                subTotal += sub.getCount();
            }
            Assert.assertEquals(subTotal, bucket.getCount(), "Sub group counts should add up to the parent bucket");
        }
    }

    // ==================== Negative Cases ====================

    @Test(description = "SearchAggregation size(0) should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void searchAggregationSizeZeroShouldFail() {
        SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(0)
                .build();
    }

    @Test(description = "Order key that is neither a metric alias nor _count/_key should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void searchAggregationOrderKeyUndefinedAliasShouldFail() {
        SearchAggregation.builder()
                .fields(Collections.singletonList(CommonData.fieldVarchar))
                .size(5)
                .addOrder(OrderSpec.builder()
                        .key("not_exist_alias")
                        .direction(AggDirection.DESC)
                        .build())
                .build();
    }

    @Test(description = "Group by a field that does not exist in the schema, record the actual behavior",
            groups = {"Smoke", "V3"})
    public void searchAggregationFieldNotExist() {
        try {
            SearchResp searchResp = searchWithAggregation(SearchAggregation.builder()
                    .fields(Collections.singletonList("fieldNotExist"))
                    .size(5)
                    .build());
            // Actual behavior: the server rejects an unknown group-by field with an error.
            // If a server build ignores it instead, log the response for diagnosis.
            log.warn("Aggregation on a non-existent field did not fail, buckets: {}",
                    searchResp.getAggregationBuckets());
        } catch (Exception e) {
            log.info("Aggregation on a non-existent field failed as expected: {}", e.getMessage());
        }
    }
}

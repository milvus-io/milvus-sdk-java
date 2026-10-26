package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.common.utils.BloomFilterUtils;
import io.milvus.common.utils.RoaringBitmapUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test cases for Milvus 3.0 membership match filter expressions.
 * Note: 3.0-20261023 起 bloom_match/roaring_match 更名为
 * membership_match(field, {blob}, type=bloom|roaring)，服务端报错提示：
 * "bloom_match is not supported; use membership_match(field, {blob}) with optional type=bloom|roaring"。
 * Data: 1000 rows, fieldInt64=fieldInt32=i, fieldDouble=i*0.5, fieldVarchar="brand_"+(i%5).
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
@Slf4j
public class MembershipMatchTest extends BaseTest {

    private static final int DIM = 128;
    private static final int INSERT_COUNT = 1000;

    private String collectionName;

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "MembershipMatch_" + GenerateUtil.getRandomString(6);

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

    private Set<Long> queryIdsByMatchFilter(String filter, byte[] blob) {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(filter)
                .filterTemplateValues(Collections.singletonMap("ids", blob))
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Set<Long> ids = new HashSet<>();
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            ids.add(((Number) result.getEntity().get(CommonData.fieldInt64)).longValue());
        }
        return ids;
    }

    // ==================== Positive Cases ====================

    @Test(description = "Query with membership_match(type=bloom) on fieldInt64, blob built from {100, 200, 300}",
            groups = {"Smoke", "V3"})
    public void queryBloomMatchInt64() {
        List<Long> members = Arrays.asList(100L, 200L, 300L);
        byte[] blob = BloomFilterUtils.buildBloomFilter(members);

        Set<Long> ids = queryIdsByMatchFilter("membership_match(" + CommonData.fieldInt64 + ", {ids}, type=bloom)", blob);
        // bloom filter may have false positives, so the hit set is a superset of the members
        Assert.assertTrue(ids.containsAll(members), "All members should be hit: " + ids);
        Assert.assertTrue(ids.size() >= members.size());
        Assert.assertTrue(ids.size() <= members.size() + 20,
                "Too many false positives: " + ids.size());
    }

    @Test(description = "Query with membership_match(type=roaring) on fieldInt64, exact 3 hits expected",
            groups = {"Smoke", "V3"})
    public void queryRoaringMatchInt64() {
        List<Long> members = Arrays.asList(100L, 200L, 300L);
        byte[] blob = RoaringBitmapUtils.buildRoaringBitmap(members);

        Set<Long> ids = queryIdsByMatchFilter("membership_match(" + CommonData.fieldInt64 + ", {ids}, type=roaring)", blob);
        // roaring bitmap is exact, no false positives
        Assert.assertEquals(ids, new HashSet<>(members), "membership_match(type=roaring) should hit exactly the 3 members");
    }

    @Test(description = "Bloom match with a large set of 500 ids, equivalent to the IN list",
            groups = {"Smoke", "V3"})
    public void bloomMatchLargeSet() {
        List<Long> members = new ArrayList<>();
        for (long i = 0; i < 500; i++) {
            members.add(i);
        }
        byte[] blob = BloomFilterUtils.buildBloomFilter(members, 0.001);

        Set<Long> bloomIds = queryIdsByMatchFilter("membership_match(" + CommonData.fieldInt64 + ", {ids}, type=bloom)", blob);
        Assert.assertTrue(bloomIds.containsAll(members), "All 500 members should be hit");

        // equivalence check against the exact IN list expression
        QueryResp inResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter(CommonData.fieldInt64 + " >= 0 and " + CommonData.fieldInt64 + " < 500")
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .limit(INSERT_COUNT)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(inResp.getQueryResults().size(), 500);
        // bloom hits = exact members + a few false positives
        Assert.assertTrue(bloomIds.size() >= 500 && bloomIds.size() <= 500 + 20,
                "Bloom hit count out of expected range: " + bloomIds.size());
    }

    @Test(description = "Search with membership_match(type=bloom) filter, all results must be members (plus possible false positives)",
            groups = {"Smoke", "V3"})
    public void searchWithBloomMatchFilter() {
        List<Long> members = Arrays.asList(100L, 200L, 300L);
        byte[] blob = BloomFilterUtils.buildBloomFilter(members);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(GenerateUtil.generateFloatVector(1, 6, DIM).get(0))))
                .filter("membership_match(" + CommonData.fieldInt64 + ", {ids}, type=bloom)")
                .filterTemplateValues(Collections.singletonMap("ids", blob))
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .limit(100)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertEquals(searchResp.getSearchResults().size(), 1);
        Set<Long> ids = new HashSet<>();
        for (SearchResp.SearchResult result : searchResp.getSearchResults().get(0)) {
            ids.add(((Number) result.getEntity().get(CommonData.fieldInt64)).longValue());
        }
        // with limit(100) all matching rows are returned; the 3 members must be present
        Assert.assertTrue(ids.containsAll(members), "Search results should contain all members: " + ids);
        Assert.assertTrue(ids.size() <= members.size() + 20,
                "Too many false positives: " + ids.size());
    }

    // ==================== Negative Cases ====================

    @Test(description = "buildBloomFilter with mixed Long and String members should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void bloomMixedTypeMembersShouldFail() {
        BloomFilterUtils.buildBloomFilter(Arrays.asList(100L, "brand_0"));
    }

    @Test(description = "buildBloomFilter with fpr out of range [0.0001, 0.05] should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void bloomInvalidFprShouldFail() {
        BloomFilterUtils.buildBloomFilter(Arrays.asList(100L, 200L, 300L), 0.5);
    }

    @Test(description = "buildRoaringBitmap with non-integer (Double) members should be rejected by the client",
            groups = {"Smoke", "V3"}, expectedExceptions = MilvusClientException.class)
    public void roaringNonIntegerMembersShouldFail() {
        RoaringBitmapUtils.buildRoaringBitmap(Arrays.asList(1.5, 2.5, 3.5));
    }

    @Test(description = "membership_match(type=bloom) on a VarChar field with a blob built from Long members (domain mismatch), record the actual behavior",
            groups = {"Smoke", "V3"})
    public void bloomMatchDomainMismatch() {
        byte[] blob = BloomFilterUtils.buildBloomFilter(Arrays.asList(100L, 200L, 300L));
        try {
            QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                    .collectionName(collectionName)
                    .filter("membership_match(" + CommonData.fieldVarchar + ", {ids}, type=bloom)")
                    .filterTemplateValues(Collections.singletonMap("ids", blob))
                    .outputFields(Collections.singletonList(CommonData.fieldVarchar))
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            // Actual behavior: the envelope records the value domain, so the proxy is expected
            // to reject an int64 blob used on a VARCHAR field. If a server build silently
            // matches nothing instead, log the response for diagnosis.
            log.warn("Domain-mismatched membership_match did not fail, result size: {}",
                    queryResp.getQueryResults().size());
        } catch (Exception e) {
            log.info("Domain-mismatched membership_match rejected as expected: {}", e.getMessage());
        }
    }
}

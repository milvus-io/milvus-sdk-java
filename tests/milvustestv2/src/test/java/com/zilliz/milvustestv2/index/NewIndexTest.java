package com.zilliz.milvustestv2.index;

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
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Test cases for Milvus 2.6 new index types:
 * NGRAM (varchar/JSON, accelerates LIKE), IVF_RABITQ, AISAQ (float vector),
 * MINHASH_LSH (binary vector with MHJACCARD metric).
 *
 * @Author yongpeng.li
 * @Date 2026
 */
public class NewIndexTest extends BaseTest {

    private static final int DIM = 128;
    private static final int TOP_K = 10;

    // collections created by each test case, dropped in @AfterClass
    private final List<String> createdCollections = new ArrayList<>();

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        for (String name : createdCollections) {
            try {
                milvusClientV2.dropCollection(DropCollectionReq.builder()
                        .collectionName(name)
                        .build());
            } catch (Exception e) {
                // ignore cleanup failure
            }
        }
    }

    // ==================== Helpers ====================

    private String createScalarAndFloatVectorCollection() {
        String name = "NewIndex_" + GenerateUtil.getRandomString(6);
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
                .maxLength(512)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldFloatVector)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
        createdCollections.add(name);
        return name;
    }

    private String createBinaryVectorCollection() {
        String name = "NewIndexBin_" + GenerateUtil.getRandomString(6);
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldBinaryVector)
                .dataType(DataType.BinaryVector)
                .dimension(DIM)
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .build());
        createdCollections.add(name);
        return name;
    }

    private static ByteBuffer generateBinaryVector(int dim) {
        byte[] bytes = new byte[dim / 8];
        new Random().nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static JsonArray toJsonArray(byte[] bytes) {
        JsonArray jsonArray = new JsonArray();
        for (byte b : bytes) {
            jsonArray.add(b);
        }
        return jsonArray;
    }

    private static List<JsonObject> generateFloatVectorRows(int count) {
        List<JsonObject> data = new ArrayList<>();
        List<List<Float>> vectors = GenerateUtil.generateFloatVector(count, 6, DIM);
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldVarchar, "varchar_" + i);
            JsonArray vector = new JsonArray();
            for (Float f : vectors.get(i)) {
                vector.add(f);
            }
            row.add(CommonData.fieldFloatVector, vector);
            data.add(row);
        }
        return data;
    }

    // ==================== Positive Cases ====================

    @Test(description = "Create NGRAM index on varchar field and query with LIKE", groups = {"Smoke"})
    public void createNgramIndexOnVarchar() {
        String collection = createScalarAndFloatVectorCollection();

        Map<String, Object> ngramParams = new HashMap<>();
        ngramParams.put("min_gram", 2);
        ngramParams.put("max_gram", 4);
        IndexParam ngramIndex = IndexParam.builder()
                .fieldName(CommonData.fieldVarchar)
                .indexName("ngram_idx")
                .indexType(IndexParam.IndexType.NGRAM)
                .extraParams(ngramParams)
                .build();
        IndexParam vectorIndex = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Arrays.asList(ngramIndex, vectorIndex))
                .build());

        // verify index type by describe
        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder()
                .collectionName(collection)
                .indexName("ngram_idx")
                .build());
        Assert.assertEquals(describeIndexResp.getIndexDescByIndexName("ngram_idx").getIndexType(),
                IndexParam.IndexType.NGRAM);

        // insert rows: first 10 rows contain substring "abc", the rest do not
        List<JsonObject> data = new ArrayList<>();
        List<List<Float>> vectors = GenerateUtil.generateFloatVector(100, 6, DIM);
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldVarchar, i < 10 ? "xxabcxx_" + i : "no_match_text_" + i);
            JsonArray vector = new JsonArray();
            for (Float f : vectors.get(i)) {
                vector.add(f);
            }
            row.add(CommonData.fieldFloatVector, vector);
            data.add(row);
        }
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collection)
                .data(data)
                .build());

        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collection)
                .build());

        // LIKE query accelerated by NGRAM index
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(collection)
                .filter(CommonData.fieldVarchar + " like \"%abc%\"")
                .outputFields(Arrays.asList(CommonData.fieldInt64, CommonData.fieldVarchar))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(queryResp.getQueryResults().size(), 10,
                "LIKE %abc% should hit exactly the 10 rows containing abc");
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            String varchar = (String) result.getEntity().get(CommonData.fieldVarchar);
            Assert.assertTrue(varchar.contains("abc"), "Hit row should contain substring abc");
        }
    }

    @Test(description = "Create IVF_RABITQ index on float vector field and search", groups = {"Smoke"})
    public void createIvfRabitqIndex() {
        String collection = createScalarAndFloatVectorCollection();

        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 128);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexName("rabitq_idx")
                .indexType(IndexParam.IndexType.IVF_RABITQ)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(extraParams)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder()
                .collectionName(collection)
                .indexName("rabitq_idx")
                .build());
        Assert.assertEquals(describeIndexResp.getIndexDescByIndexName("rabitq_idx").getIndexType(),
                IndexParam.IndexType.IVF_RABITQ);

        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collection)
                .data(generateFloatVectorRows(1000))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collection)
                .build());

        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collection)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(TOP_K)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertEquals(searchResp.getSearchResults().get(0).size(), TOP_K);
    }

    @Test(description = "Create AISAQ index on float vector field and search", groups = {"Smoke"})
    public void createAisaqIndex() {
        String collection = createScalarAndFloatVectorCollection();

        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 128);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexName("aisaq_idx")
                .indexType(IndexParam.IndexType.AISAQ)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(extraParams)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder()
                .collectionName(collection)
                .indexName("aisaq_idx")
                .build());
        Assert.assertEquals(describeIndexResp.getIndexDescByIndexName("aisaq_idx").getIndexType(),
                IndexParam.IndexType.AISAQ);

        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collection)
                .data(generateFloatVectorRows(1000))
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collection)
                .build());

        List<Float> queryVector = GenerateUtil.generateFloatVector(1, 6, DIM).get(0);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collection)
                .annsField(CommonData.fieldFloatVector)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .topK(TOP_K)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        Assert.assertEquals(searchResp.getSearchResults().get(0).size(), TOP_K);
    }

    @Test(description = "Create MINHASH_LSH index on binary vector field with MHJACCARD metric and search", groups = {"Smoke"})
    public void createMinHashLshIndex() {
        String collection = createBinaryVectorCollection();

        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("mh_lsh_band", 4);
        extraParams.put("with_raw_data", true);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldBinaryVector)
                .indexName("minhash_idx")
                .indexType(IndexParam.IndexType.MINHASH_LSH)
                .metricType(IndexParam.MetricType.MHJACCARD)
                .extraParams(extraParams)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        DescribeIndexResp describeIndexResp = milvusClientV2.describeIndex(DescribeIndexReq.builder()
                .collectionName(collection)
                .indexName("minhash_idx")
                .build());
        Assert.assertEquals(describeIndexResp.getIndexDescByIndexName("minhash_idx").getIndexType(),
                IndexParam.IndexType.MINHASH_LSH);

        // insert binary vectors; keep the raw bytes of row id=7 so the query below
        // can use the identical vector (Jaccard distance 0) for a deterministic hit
        byte[] queryBytes = null;
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            byte[] bytes = generateBinaryVector(DIM).array();
            if (i == 7) {
                queryBytes = bytes;
            }
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.add(CommonData.fieldBinaryVector, toJsonArray(bytes));
            data.add(row);
        }
        milvusClientV2.insert(InsertReq.builder()
                .collectionName(collection)
                .data(data)
                .build());
        milvusClientV2.loadCollection(LoadCollectionReq.builder()
                .collectionName(collection)
                .build());

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collection)
                .annsField(CommonData.fieldBinaryVector)
                .data(Collections.singletonList(new BinaryVec(ByteBuffer.wrap(queryBytes))))
                .topK(TOP_K)
                .metricType(IndexParam.MetricType.MHJACCARD)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        // identical signature falls into the same LSH band bucket, so row id=7 must be top1
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "Searching with an inserted binary vector should hit at least itself");
        Assert.assertEquals(String.valueOf(searchResp.getSearchResults().get(0).get(0).getId()), "7",
                "Top1 should be the row with identical bytes (Jaccard distance 0)");
    }

    // ==================== Negative Cases ====================

    @Test(description = "Create NGRAM index on Int64 field should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void createNgramIndexOnInt64FieldShouldFail() {
        String collection = createScalarAndFloatVectorCollection();
        Map<String, Object> ngramParams = new HashMap<>();
        ngramParams.put("min_gram", 2);
        ngramParams.put("max_gram", 4);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldInt64)
                .indexType(IndexParam.IndexType.NGRAM)
                .extraParams(ngramParams)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    @Test(description = "Create NGRAM index with min_gram > max_gram should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void createNgramIndexWithInvalidGramParamsShouldFail() {
        String collection = createScalarAndFloatVectorCollection();
        Map<String, Object> ngramParams = new HashMap<>();
        ngramParams.put("min_gram", 5);
        ngramParams.put("max_gram", 2);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldVarchar)
                .indexType(IndexParam.IndexType.NGRAM)
                .extraParams(ngramParams)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    @Test(description = "Create MINHASH_LSH index with L2 metric should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void createMinHashLshWithWrongMetricShouldFail() {
        String collection = createBinaryVectorCollection();
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldBinaryVector)
                .indexType(IndexParam.IndexType.MINHASH_LSH)
                .metricType(IndexParam.MetricType.L2)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    @Test(description = "Create MINHASH_LSH index on float vector field should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void createMinHashLshOnFloatVectorFieldShouldFail() {
        String collection = createScalarAndFloatVectorCollection();
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.MINHASH_LSH)
                .metricType(IndexParam.MetricType.MHJACCARD)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    @Test(description = "Create IVF_RABITQ index with HAMMING metric on float vector should fail", groups = {"Smoke"},
            expectedExceptions = Exception.class)
    public void createIvfRabitqWithHammingMetricShouldFail() {
        String collection = createScalarAndFloatVectorCollection();
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 128);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldFloatVector)
                .indexType(IndexParam.IndexType.IVF_RABITQ)
                .metricType(IndexParam.MetricType.HAMMING)
                .extraParams(extraParams)
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }
}

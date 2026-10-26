package com.zilliz.milvustestv2.structArray;

import com.google.gson.JsonArray;
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
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BFloat16Vec;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.EmbeddingList;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import io.milvus.v2.service.vector.request.data.Int8Vec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Milvus 3.0 struct enhancement: struct sub-fields support BinaryVector / Float16Vector /
 * BFloat16Vector / Int8Vector (2.6 only supports FloatVector).
 * Data format: struct = JSON array of objects; binary / f16 / bf16 / int8 vectors are written
 * as JSON arrays of numbers (bytes).
 * Index metric inside struct uses the MAX_SIM_* family:
 *   binary -> MAX_SIM_JACCARD / MAX_SIM_HAMMING, float family -> MAX_SIM_COSINE / MAX_SIM_L2 / MAX_SIM_IP.
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
public class StructMultiVectorTypeTest extends BaseTest {

    private String collectionName;
    private static final int DIM = 128;
    private static final int INSERT_COUNT = 200;
    private static final int ELEMENTS_PER_ROW = 3;

    private static final String structFieldBinary = "structBinaryVector";
    private static final String structFieldF16 = "structFloat16Vector";
    private static final String structFieldBF16 = "structBFloat16Vector";
    private static final String structFieldInt8 = "structInt8Vector";

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "StructMultiVector_" + GenerateUtil.getRandomString(6);
        createMultiVectorStructCollection(collectionName);

        List<JsonObject> data = generateMultiVectorStructData(INSERT_COUNT);
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT, "Insert count should match");

        // HNSW + MAX_SIM_* index on each struct vector sub-field (required before load)
        createStructIndex(collectionName, structFieldBinary, "idx_binary", IndexParam.MetricType.MAX_SIM_JACCARD);
        createStructIndex(collectionName, structFieldF16, "idx_f16", IndexParam.MetricType.MAX_SIM_COSINE);
        createStructIndex(collectionName, structFieldBF16, "idx_bf16", IndexParam.MetricType.MAX_SIM_IP);
        createStructIndex(collectionName, structFieldInt8, "idx_int8", IndexParam.MetricType.MAX_SIM_COSINE);

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

    // ==================== Helpers ====================

    private static void createMultiVectorStructCollection(String name) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldInt64)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldStruct)
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(CommonData.structMaxCapacity)
                .addStructField(AddFieldReq.builder()
                        .fieldName(CommonData.structFieldInt32)
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(structFieldBinary)
                        .dataType(DataType.BinaryVector)
                        .dimension(DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(structFieldF16)
                        .dataType(DataType.Float16Vector)
                        .dimension(DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(structFieldBF16)
                        .dataType(DataType.BFloat16Vector)
                        .dimension(DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(structFieldInt8)
                        .dataType(DataType.Int8Vector)
                        .dimension(DIM)
                        .build())
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(name)
                .collectionSchema(schema)
                .enableDynamicField(false)
                .numShards(1)
                .build());
    }

    private static List<JsonObject> generateMultiVectorStructData(int count) {
        List<JsonObject> dataList = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            JsonArray structArray = new JsonArray();
            for (int j = 0; j < ELEMENTS_PER_ROW; j++) {
                JsonObject struct = new JsonObject();
                struct.addProperty(CommonData.structFieldInt32, j);
                struct.add(structFieldBinary, bytesToJsonArray(CommonFunction.generateBinaryVector(DIM).array()));
                struct.add(structFieldF16, bytesToJsonArray(CommonFunction.generateFloat16Vector(DIM).array()));
                struct.add(structFieldBF16, bytesToJsonArray(CommonFunction.generateBF16Vector(DIM).array()));
                struct.add(structFieldInt8, bytesToJsonArray(generateInt8Vector(DIM, random)));
                structArray.add(struct);
            }
            row.add(CommonData.fieldStruct, structArray);
            dataList.add(row);
        }
        return dataList;
    }

    private static byte[] generateInt8Vector(int dim, Random random) {
        byte[] vector = new byte[dim];
        random.nextBytes(vector);
        return vector;
    }

    private static JsonArray bytesToJsonArray(byte[] bytes) {
        JsonArray jsonArray = new JsonArray();
        for (byte b : bytes) {
            jsonArray.add(b);
        }
        return jsonArray;
    }

    private static void createStructIndex(String collection, String subVectorField, String indexName,
                                          IndexParam.MetricType metricType) {
        String fieldPath = String.format("%s[%s]", CommonData.fieldStruct, subVectorField);
        IndexParam indexParam = IndexParam.builder()
                .fieldName(fieldPath)
                .indexName(indexName)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(metricType)
                .extraParams(new HashMap<String, Object>() {{
                    put("M", 16);
                    put("efConstruction", 200);
                }})
                .build();
        milvusClientV2.createIndex(CreateIndexReq.builder()
                .collectionName(collection)
                .indexParams(Collections.singletonList(indexParam))
                .build());
    }

    private static String structAnnsField(String subVectorField) {
        return String.format("%s[%s]", CommonData.fieldStruct, subVectorField);
    }

    private static EmbeddingList buildEmbeddingList(int vectorCount, DataType vectorType) {
        EmbeddingList embeddingList = new EmbeddingList();
        Random random = new Random();
        for (int i = 0; i < vectorCount; i++) {
            switch (vectorType) {
                case BinaryVector:
                    embeddingList.add(new BinaryVec(CommonFunction.generateBinaryVector(DIM)));
                    break;
                case Float16Vector:
                    embeddingList.add(new Float16Vec(CommonFunction.generateFloat16Vector(DIM)));
                    break;
                case BFloat16Vector:
                    embeddingList.add(new BFloat16Vec(CommonFunction.generateBF16Vector(DIM)));
                    break;
                case Int8Vector:
                    embeddingList.add(new Int8Vec(generateInt8Vector(DIM, random)));
                    break;
                default:
                    throw new IllegalArgumentException("unsupported vector type: " + vectorType);
            }
        }
        return embeddingList;
    }

    private void assertStructSearchNonEmpty(String subVectorField, DataType vectorType) {
        EmbeddingList embeddingList = buildEmbeddingList(3, vectorType);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(structAnnsField(subVectorField))
                .data(Collections.singletonList(embeddingList))
                .topK(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Collections.singletonList(CommonData.fieldInt64))
                .build());
        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty(),
                "search on struct sub-field " + subVectorField + " should return results");
        Assert.assertFalse(searchResp.getSearchResults().get(0).isEmpty(),
                "search on struct sub-field " + subVectorField + " should return non-empty hits");
        Assert.assertTrue(searchResp.getSearchResults().get(0).size() <= 10,
                "result size should not exceed topK");
    }

    // ==================== Positive Tests ====================

    @Test(description = "Search on struct BinaryVector sub-field with EmbeddingList of BinaryVec",
            groups = {"Smoke", "V3"})
    public void searchStructBinaryVectorField() {
        assertStructSearchNonEmpty(structFieldBinary, DataType.BinaryVector);
    }

    @Test(description = "Search on struct Float16Vector sub-field with EmbeddingList of Float16Vec",
            groups = {"Smoke", "V3"})
    public void searchStructFloat16VectorField() {
        assertStructSearchNonEmpty(structFieldF16, DataType.Float16Vector);
    }

    @Test(description = "Search on struct BFloat16Vector sub-field with EmbeddingList of BFloat16Vec",
            groups = {"Smoke", "V3"})
    public void searchStructBFloat16VectorField() {
        assertStructSearchNonEmpty(structFieldBF16, DataType.BFloat16Vector);
    }

    @Test(description = "Search on struct Int8Vector sub-field with EmbeddingList of Int8Vec",
            groups = {"Smoke", "V3"})
    public void searchStructInt8VectorField() {
        assertStructSearchNonEmpty(structFieldInt8, DataType.Int8Vector);
    }

    // ==================== Negative Tests ====================

    @Test(description = "Binary struct sub-vector with float metric MAX_SIM_COSINE should fail",
            groups = {"Smoke", "V3"})
    public void binarySubVectorWithFloatMetricShouldFail() {
        String negativeCollection = "StructMetricMismatch_" + GenerateUtil.getRandomString(6);
        try {
            createMultiVectorStructCollection(negativeCollection);
            List<JsonObject> data = generateMultiVectorStructData(10);
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(negativeCollection)
                    .data(data)
                    .build());
            // binary vector only supports MAX_SIM_JACCARD / MAX_SIM_HAMMING inside struct
            createStructIndex(negativeCollection, structFieldBinary, "idx_bad_metric",
                    IndexParam.MetricType.MAX_SIM_COSINE);
            Assert.fail("Should throw exception for binary struct sub-vector with MAX_SIM_COSINE");
        } catch (Exception e) {
            // expected: metric type not match for binary vector
            Assert.assertTrue(true, "expected exception: " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(negativeCollection)
                    .build());
        }
    }

    @Test(description = "Insert struct data whose sub-vector dim does not match schema should fail",
            groups = {"Smoke", "V3"})
    public void insertDimMismatchStructDataShouldFail() {
        List<JsonObject> dataList = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty(CommonData.fieldInt64, 100000L);
        JsonArray structArray = new JsonArray();
        JsonObject struct = new JsonObject();
        struct.addProperty(CommonData.structFieldInt32, 0);
        // binary sub-vector with dim=64 (8 bytes) instead of dim=128 (16 bytes)
        struct.add(structFieldBinary, bytesToJsonArray(CommonFunction.generateBinaryVector(DIM / 2).array()));
        struct.add(structFieldF16, bytesToJsonArray(CommonFunction.generateFloat16Vector(DIM).array()));
        struct.add(structFieldBF16, bytesToJsonArray(CommonFunction.generateBF16Vector(DIM).array()));
        struct.add(structFieldInt8, bytesToJsonArray(generateInt8Vector(DIM, new Random())));
        structArray.add(struct);
        row.add(CommonData.fieldStruct, structArray);
        dataList.add(row);

        try {
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(dataList)
                    .build());
            Assert.fail("Should throw exception for struct sub-vector dim mismatch");
        } catch (Exception e) {
            Assert.assertTrue(true, "expected exception: " + e.getMessage());
        }
    }
}

package com.zilliz.milvustestv2.collection;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
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
import java.util.Map;
import java.util.Random;

/**
 * Test cases for MINHASH function (Milvus 3.0 feature, text approximate dedup by Jaccard similarity).
 * VarChar text field + BinaryVector signature field (dim = num_hashes * 32) + MINHASH function,
 * MINHASH_LSH index with MHJACCARD metric, search with EmbeddedText.
 *
 * @Author yongpeng.li
 * @Date 2026/10/26
 */
public class MinHashFunctionTest extends BaseTest {

    private String minhashCollectionName;
    private static final String ID_FIELD = "id";
    private static final String TEXT_FIELD = "text";
    private static final String SIGNATURE_FIELD = "minhash_signature";
    private static final String FUNCTION_NAME = "text_to_minhash";
    private static final int NUM_HASHES = 16;
    private static final int SIGNATURE_DIM = NUM_HASHES * 32;
    private static final int INSERT_COUNT = 100;

    // id = 1 holds BASE_TEXT; the search query differs from it by exactly one word
    private static final String BASE_TEXT =
            "the quick brown fox jumps over the lazy dog while the sun shines brightly over the green hills near the wide river";
    private static final String NEAR_DUP_TEXT =
            "the quick brown fox jumps over the lazy dog while the sun glows brightly over the green hills near the wide river";

    private static final String[] WORD_POOL = {
            "apple", "banana", "orange", "grape", "melon", "table", "chair", "window", "door", "lamp",
            "road", "bridge", "tower", "garden", "market", "cloud", "rain", "snow", "wind", "storm",
            "piano", "guitar", "violin", "drum", "flute", "novel", "poem", "essay", "letter", "diary",
            "engine", "motor", "wheel", "brake", "mirror", "rocket", "satellite", "orbit", "planet", "comet",
            "tiger", "lion", "zebra", "giraffe", "panda", "eagle", "falcon", "shark", "whale", "dolphin"
    };

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        minhashCollectionName = "MinHash_" + GenerateUtil.getRandomString(6);
    }

    @AfterClass(alwaysRun = true)
    public void cleanTestData() {
        if (minhashCollectionName != null) {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(minhashCollectionName)
                    .build());
        }
    }

    private static String randomSentence(Random random) {
        StringBuilder sb = new StringBuilder();
        int wordCount = 8 + random.nextInt(5);
        for (int i = 0; i < wordCount; i++) {
            sb.append(WORD_POOL[random.nextInt(WORD_POOL.length)]).append(" ");
        }
        return sb.toString().trim();
    }

    private static void createMinHashCollection(String collectionName) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(TEXT_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(SIGNATURE_FIELD)
                .dataType(DataType.BinaryVector)
                .dimension(SIGNATURE_DIM)
                .build());
        schema.addFunction(CreateCollectionReq.Function.builder()
                .name(FUNCTION_NAME)
                .functionType(FunctionType.MINHASH)
                .inputFieldNames(Collections.singletonList(TEXT_FIELD))
                .outputFieldNames(Collections.singletonList(SIGNATURE_FIELD))
                .param("num_hashes", String.valueOf(NUM_HASHES))
                .param("shingle_size", "3")
                .param("token_level", "word")
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(SIGNATURE_FIELD)
                .indexType(IndexParam.IndexType.MINHASH_LSH)
                .metricType(IndexParam.MetricType.MHJACCARD)
                .extraParams(new HashMap<String, Object>() {{
                    put("mh_lsh_band", 8);
                    put("with_raw_data", true);
                }})
                .build());

        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .indexParams(indexes)
                .build());
    }

    // ==================== Positive Tests ====================

    @Test(description = "Create collection with MINHASH function and verify schema", groups = {"Smoke", "V3"})
    public void createMinHashCollectionSuccess() {
        createMinHashCollection(minhashCollectionName);

        DescribeCollectionResp descResp = milvusClientV2.describeCollection(
                DescribeCollectionReq.builder().collectionName(minhashCollectionName).build());
        Assert.assertTrue(descResp.getFieldNames().contains(SIGNATURE_FIELD),
                "BinaryVector signature field should exist");

        List<CreateCollectionReq.Function> functionList = descResp.getCollectionSchema().getFunctionList();
        Assert.assertNotNull(functionList, "functionList should not be null");
        boolean found = false;
        for (CreateCollectionReq.Function function : functionList) {
            if (FUNCTION_NAME.equals(function.getName())) {
                found = true;
                Assert.assertEquals(function.getFunctionType(), FunctionType.MINHASH);
                Assert.assertEquals(function.getInputFieldNames(), Collections.singletonList(TEXT_FIELD));
                Assert.assertEquals(function.getOutputFieldNames(), Collections.singletonList(SIGNATURE_FIELD));
            }
        }
        Assert.assertTrue(found, "functionList should contain the MINHASH function");
    }

    @Test(description = "Insert 100 rows, only id and text are given, signature is generated by function", groups = {"Smoke", "V3"}, dependsOnMethods = {"createMinHashCollectionSuccess"})
    public void insertMinHashDataSuccess() {
        List<JsonObject> rows = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < INSERT_COUNT; i++) {
            JsonObject row = new JsonObject();
            long id = i + 1L;
            row.addProperty(ID_FIELD, id);
            if (id == 1L) {
                row.addProperty(TEXT_FIELD, BASE_TEXT);
            } else {
                row.addProperty(TEXT_FIELD, randomSentence(random));
            }
            // do not write the signature column, it is generated by the MINHASH function
            rows.add(row);
        }
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(minhashCollectionName)
                .data(rows)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT, "Insert count should match");
    }

    @Test(description = "Query and verify the BinaryVector signature field is generated", groups = {"Smoke", "V3"}, dependsOnMethods = {"insertMinHashDataSuccess"})
    public void querySignatureFieldGenerated() {
        QueryResp queryResp = milvusClientV2.query(QueryReq.builder()
                .collectionName(minhashCollectionName)
                .filter(ID_FIELD + " in [1, 2, 3]")
                .outputFields(Arrays.asList(ID_FIELD, TEXT_FIELD, SIGNATURE_FIELD))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        Assert.assertEquals(queryResp.getQueryResults().size(), 3);
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object signature = result.getEntity().get(SIGNATURE_FIELD);
            Assert.assertTrue(signature instanceof ByteBuffer,
                    "Signature field should read back as ByteBuffer, but got: "
                            + (signature == null ? "null" : signature.getClass().getName()));
            // SDK 3.0.10 返回的 ByteBuffer 未 flip，remaining()=0，此处按 capacity 校验
            // （SDK 已知问题，与 2.6 Int8Vector 同 bug，参考 Int8VectorTest）
            Assert.assertEquals(((ByteBuffer) signature).capacity(), SIGNATURE_DIM / 8,
                    "Signature byte length should be dim/8");
        }
    }

    @Test(description = "Search with EmbeddedText for a near-duplicate text, the most similar row should rank top1", groups = {"Smoke", "V3"}, dependsOnMethods = {"insertMinHashDataSuccess"})
    public void searchNearDuplicateTop1() {
        Map<String, Object> searchParams = new HashMap<>();
        searchParams.put("mh_search_with_jaccard", true);
        searchParams.put("refine_k", 50);

        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(minhashCollectionName)
                .annsField(SIGNATURE_FIELD)
                .data(Collections.singletonList(new EmbeddedText(NEAR_DUP_TEXT)))
                .metricType(IndexParam.MetricType.MHJACCARD)
                .searchParams(searchParams)
                .limit(5)
                .outputFields(Arrays.asList(ID_FIELD, TEXT_FIELD))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        List<SearchResp.SearchResult> hits = searchResp.getSearchResults().get(0);
        Assert.assertFalse(hits.isEmpty(), "Search should return results");
        // NEAR_DUP_TEXT differs from BASE_TEXT (id=1) by only one word, Jaccard similarity is by far the highest
        Long top1Id = ((Number) hits.get(0).getEntity().get(ID_FIELD)).longValue();
        Assert.assertEquals(top1Id, 1L,
                "The near-duplicate row id=1 should rank top1, but got id=" + top1Id);
    }

    // ==================== Negative Tests ====================

    @Test(description = "Insert row with manually provided signature column, behavior recorded", groups = {"Smoke", "V3"}, dependsOnMethods = {"insertMinHashDataSuccess"})
    public void insertManualSignatureBehavior() {
        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, 1000L);
        row.addProperty(TEXT_FIELD, "manual signature behavior record text");
        // manually provide the function output column: server should reject it or overwrite it
        byte[] manualSignature = new byte[SIGNATURE_DIM / 8];
        new Random().nextBytes(manualSignature);
        row.add(SIGNATURE_FIELD, new Gson().toJsonTree(manualSignature));
        try {
            InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                    .collectionName(minhashCollectionName)
                    .data(Collections.singletonList(row))
                    .build());
            // behavior record: insert accepted, function output column overwritten/ignored by server
            System.out.println("Insert with manual signature column was ACCEPTED, insertCnt="
                    + insertResp.getInsertCnt() + " (behavior recorded)");
        } catch (Exception e) {
            // behavior record: server rejects writing a function output column
            System.out.println("Insert with manual signature column was REJECTED: "
                    + e.getClass().getName() + ": " + e.getMessage());
        }
    }

    @Test(description = "MINHASH function with input field not existing should fail", groups = {"Smoke", "V3"})
    public void minhashInputNotExistFieldShouldFail() {
        String collectionName = "MinHashBadInput_" + GenerateUtil.getRandomString(6);
        try {
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName(ID_FIELD)
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(false)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(TEXT_FIELD)
                    .dataType(DataType.VarChar)
                    .maxLength(65535)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(SIGNATURE_FIELD)
                    .dataType(DataType.BinaryVector)
                    .dimension(SIGNATURE_DIM)
                    .build());
            // input field does not exist in schema
            schema.addFunction(CreateCollectionReq.Function.builder()
                    .name(FUNCTION_NAME)
                    .functionType(FunctionType.MINHASH)
                    .inputFieldNames(Collections.singletonList("not_exist_field"))
                    .outputFieldNames(Collections.singletonList(SIGNATURE_FIELD))
                    .param("num_hashes", String.valueOf(NUM_HASHES))
                    .param("shingle_size", "3")
                    .param("token_level", "word")
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());
            Assert.fail("MINHASH function with non-existent input field should throw exception");
        } catch (Exception e) {
            // expected: client or server side validation
            System.out.println("Expected exception for MINHASH with non-existent input field: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }

    @Test(description = "MINHASH function with illegal num_hashes value should fail", groups = {"Smoke", "V3"})
    public void invalidNumHashesShouldFail() {
        String collectionName = "MinHashBadParam_" + GenerateUtil.getRandomString(6);
        try {
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName(ID_FIELD)
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(false)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(TEXT_FIELD)
                    .dataType(DataType.VarChar)
                    .maxLength(65535)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName(SIGNATURE_FIELD)
                    .dataType(DataType.BinaryVector)
                    .dimension(SIGNATURE_DIM)
                    .build());
            // num_hashes must be a positive integer, "abc" is illegal
            schema.addFunction(CreateCollectionReq.Function.builder()
                    .name(FUNCTION_NAME)
                    .functionType(FunctionType.MINHASH)
                    .inputFieldNames(Collections.singletonList(TEXT_FIELD))
                    .outputFieldNames(Collections.singletonList(SIGNATURE_FIELD))
                    .param("num_hashes", "abc")
                    .param("shingle_size", "3")
                    .param("token_level", "word")
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());
            Assert.fail("MINHASH function with illegal num_hashes should throw exception");
        } catch (Exception e) {
            // expected: client or server side validation of function params
            System.out.println("Expected exception for MINHASH with illegal num_hashes: "
                    + e.getClass().getName() + ": " + e.getMessage());
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
        }
    }
}

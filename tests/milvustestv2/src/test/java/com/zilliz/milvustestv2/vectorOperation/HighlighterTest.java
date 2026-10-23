package com.zilliz.milvustestv2.vectorOperation;

import com.google.gson.JsonObject;
import com.zilliz.milvustestv2.common.BaseTest;
import com.zilliz.milvustestv2.common.CommonData;
import com.zilliz.milvustestv2.utils.GenerateUtil;
import io.milvus.common.clientenum.FunctionType;
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
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.highlighter.LexicalHighlighter;
import io.milvus.v2.service.vector.request.highlighter.SemanticHighlighter;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test cases for search result highlighter (Milvus 2.6, server side requires >= 2.6.9)
 * LexicalHighlighter needs a collection with analyzer + BM25 function;
 * SemanticHighlighter depends on external model service which is absent in test env (negative case only)
 *
 * @Author yongpeng.li
 * @Date 2026/10/23
 */
public class HighlighterTest extends BaseTest {

    private String collectionName;
    private static final int INSERT_COUNT = 100;
    private static final String PRE_TAG = "<em>";
    private static final String POST_TAG = "</em>";

    @BeforeClass(alwaysRun = true)
    public void initTestData() {
        collectionName = "Highlighter_" + GenerateUtil.getRandomString(6);

        // schema: Int64 pk + varchar text field (analyzer enabled) + sparse vector + BM25 function
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
                .enableAnalyzer(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(CommonData.fieldSparseVector)
                .dataType(DataType.SparseFloatVector)
                .build());
        schema.addFunction(CreateCollectionReq.Function.builder()
                .name("bm25Function")
                .functionType(FunctionType.BM25)
                .inputFieldNames(Collections.singletonList(CommonData.fieldVarchar))
                .outputFieldNames(Collections.singletonList(CommonData.fieldSparseVector))
                .build());
        milvusClientV2.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());

        // insert english text, every document contains the word "milvus"
        List<JsonObject> data = new ArrayList<>();
        for (int i = 0; i < INSERT_COUNT; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(CommonData.fieldInt64, (long) i);
            row.addProperty(CommonData.fieldVarchar, "milvus vector database highlighter test document " + i
                    + " for lexical search highlight verification");
            data.add(row);
        }
        InsertResp insertResp = milvusClientV2.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
        Assert.assertEquals(insertResp.getInsertCnt(), INSERT_COUNT);

        // sparse index for the BM25 output field
        IndexParam indexParam = IndexParam.builder()
                .fieldName(CommonData.fieldSparseVector)
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25)
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

    // ==================== private helpers ====================

    // BM25 function based search requires raw text as query, not SparseFloatVec
    private static List<BaseVector> buildTextSearchData(String text) {
        return Collections.singletonList(new EmbeddedText(text));
    }

    private static LexicalHighlighter buildLexicalHighlighter(String field, String text,
                                                              String preTag, String postTag) {
        return LexicalHighlighter.builder()
                .addHighlightQuery(new LexicalHighlighter.HighlightQuery("TextMatch", field, text))
                .preTags(Collections.singletonList(preTag))
                .postTags(Collections.singletonList(postTag))
                .build();
    }

    // ==================== Positive cases ====================

    @Test(description = "Search with LexicalHighlighter on BM25 collection", groups = {"Smoke"})
    public void searchWithLexicalHighlighter() {
        LexicalHighlighter highlighter = buildLexicalHighlighter(
                CommonData.fieldVarchar, "milvus", PRE_TAG, POST_TAG);
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldSparseVector)
                .data(buildTextSearchData("milvus"))
                .highlighter(highlighter)
                .outputFields(Collections.singletonList(CommonData.fieldVarchar))
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        List<SearchResp.SearchResult> results = searchResp.getSearchResults().get(0);
        Assert.assertFalse(results.isEmpty());

        // every document contains "milvus", so highlight results should be returned
        boolean highlighted = false;
        for (SearchResp.SearchResult result : results) {
            Map<String, SearchResp.HighlightResult> highlightResults = result.getHighlightResults();
            Assert.assertNotNull(highlightResults);
            if (highlightResults.containsKey(CommonData.fieldVarchar)) {
                SearchResp.HighlightResult highlightResult = result.getHighlightResult(CommonData.fieldVarchar);
                Assert.assertNotNull(highlightResult);
                Assert.assertFalse(highlightResult.getFragments().isEmpty(),
                        "Highlight fragments should not be empty");
                for (String fragment : highlightResult.getFragments()) {
                    Assert.assertTrue(fragment.contains(PRE_TAG),
                            "Fragment should contain pre tag: " + fragment);
                }
                highlighted = true;
            }
        }
        Assert.assertTrue(highlighted, "At least one result should carry highlight results of the text field");
    }

    @Test(description = "Search with LexicalHighlighter with custom tags and fragmentSize", groups = {"Smoke"})
    public void searchWithLexicalHighlighterCustomTags() {
        LexicalHighlighter highlighter = LexicalHighlighter.builder()
                .addHighlightQuery(new LexicalHighlighter.HighlightQuery("TextMatch", CommonData.fieldVarchar, "milvus"))
                .preTags(Collections.singletonList("[hl]"))
                .postTags(Collections.singletonList("[/hl]"))
                .fragmentSize(64)
                .build();
        SearchResp searchResp = milvusClientV2.search(SearchReq.builder()
                .collectionName(collectionName)
                .annsField(CommonData.fieldSparseVector)
                .data(buildTextSearchData("milvus"))
                .highlighter(highlighter)
                .outputFields(Collections.singletonList(CommonData.fieldVarchar))
                .limit(10)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        Assert.assertNotNull(searchResp);
        Assert.assertFalse(searchResp.getSearchResults().isEmpty());
        List<SearchResp.SearchResult> results = searchResp.getSearchResults().get(0);
        Assert.assertFalse(results.isEmpty());

        boolean highlighted = false;
        for (SearchResp.SearchResult result : results) {
            SearchResp.HighlightResult highlightResult = result.getHighlightResult(CommonData.fieldVarchar);
            if (highlightResult != null && !highlightResult.getFragments().isEmpty()) {
                Assert.assertTrue(highlightResult.getFragments().get(0).contains("[hl]"),
                        "Fragment should contain custom pre tag: " + highlightResult.getFragments().get(0));
                highlighted = true;
            }
        }
        Assert.assertTrue(highlighted, "At least one result should carry highlight results with custom tags");
    }

    // ==================== Negative cases ====================

    @Test(description = "HighlightQuery with non-existent field should fail", groups = {"Smoke"})
    public void highlightNonExistentFieldShouldFail() {
        try {
            LexicalHighlighter highlighter = buildLexicalHighlighter(
                    "notExistField", "milvus", PRE_TAG, POST_TAG);
            milvusClientV2.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .annsField(CommonData.fieldSparseVector)
                    .data(buildTextSearchData("milvus"))
                    .highlighter(highlighter)
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when highlight field does not exist");
        } catch (Exception e) {
            // expected exception
        }
    }

    @Test(description = "LexicalHighlighter on collection without analyzer should fail", groups = {"Smoke"})
    public void highlightOnNonAnalyzerCollectionShouldFail() {
        String plainCollection = "PlainHighlight_" + GenerateUtil.getRandomString(6);
        try {
            // plain collection: varchar field without analyzer + float vector
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
                    .dimension(CommonData.dim)
                    .build());
            milvusClientV2.createCollection(CreateCollectionReq.builder()
                    .collectionName(plainCollection)
                    .collectionSchema(schema)
                    .build());

            List<JsonObject> data = new ArrayList<>();
            List<List<Float>> vectors = GenerateUtil.generateFloatVector(10, 6, CommonData.dim);
            for (int i = 0; i < 10; i++) {
                JsonObject row = new JsonObject();
                row.addProperty(CommonData.fieldInt64, (long) i);
                row.addProperty(CommonData.fieldVarchar, "plain text document " + i);
                com.google.gson.JsonArray vec = new com.google.gson.JsonArray();
                for (Float v : vectors.get(i)) {
                    vec.add(v);
                }
                row.add(CommonData.fieldFloatVector, vec);
                data.add(row);
            }
            milvusClientV2.insert(InsertReq.builder()
                    .collectionName(plainCollection)
                    .data(data)
                    .build());
            IndexParam indexParam = IndexParam.builder()
                    .fieldName(CommonData.fieldFloatVector)
                    .indexType(IndexParam.IndexType.HNSW)
                    .metricType(IndexParam.MetricType.L2)
                    .build();
            milvusClientV2.createIndex(CreateIndexReq.builder()
                    .collectionName(plainCollection)
                    .indexParams(Collections.singletonList(indexParam))
                    .build());
            milvusClientV2.loadCollection(LoadCollectionReq.builder()
                    .collectionName(plainCollection)
                    .build());

            LexicalHighlighter highlighter = buildLexicalHighlighter(
                    CommonData.fieldVarchar, "plain", PRE_TAG, POST_TAG);
            List<BaseVector> searchData = new ArrayList<>();
            searchData.add(new FloatVec(GenerateUtil.generateFloatVector(1, 6, CommonData.dim).get(0)));
            milvusClientV2.search(SearchReq.builder()
                    .collectionName(plainCollection)
                    .annsField(CommonData.fieldFloatVector)
                    .data(searchData)
                    .highlighter(highlighter)
                    .limit(5)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when highlighting a field without analyzer");
        } catch (Exception e) {
            // expected exception
        } finally {
            milvusClientV2.dropCollection(DropCollectionReq.builder()
                    .collectionName(plainCollection)
                    .build());
        }
    }

    @Test(description = "SemanticHighlighter without model service should fail", groups = {"Smoke"})
    public void semanticHighlighterShouldFail() {
        try {
            // there is no model deployment in test env, search with semantic highlighter should fail
            SemanticHighlighter highlighter = SemanticHighlighter.builder()
                    .queries(Collections.singletonList("milvus vector database"))
                    .inputFields(Collections.singletonList(CommonData.fieldVarchar))
                    .preTags(Collections.singletonList(PRE_TAG))
                    .postTags(Collections.singletonList(POST_TAG))
                    .build();
            milvusClientV2.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .annsField(CommonData.fieldSparseVector)
                    .data(buildTextSearchData("milvus"))
                    .highlighter(highlighter)
                    .limit(10)
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            Assert.fail("Should throw exception when semantic highlighter model service is unavailable");
        } catch (Exception e) {
            // expected exception
        }
    }
}

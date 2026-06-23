/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.Function;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.request.highlighter.LexicalHighlighter;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates lexical highlighter usage with cross-field behavior.
 *
 * This example builds the BM25 sparse vector from the `title` field, so ranking is driven by
 * title matches. At the same time, it uses `TEXT_MATCH(text, ...)` plus `LexicalHighlighter`
 * to highlight matched terms in the `text` field. In other words, the search field and the
 * highlighted field are intentionally different.
 *
 * Prerequisites:
 * - A running Milvus instance on localhost:19530
 * - Server-side text search and highlighter support enabled
 */
public class HighlighterExample {
    private static final String COLLECTION_NAME = "java_sdk_example_highlighter_v2";
    private static final String ID_FIELD = "id";
    private static final String TITLE_FIELD = "title";
    private static final String VECTOR_FIELD = "vector";
    private static final String TEXT_FIELD = "text";

    private static MilvusClientV2 connect() {
        return new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void createCollection(MilvusClientV2 client) {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(TITLE_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(512)
                .enableAnalyzer(true)
                .enableMatch(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(TEXT_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .enableAnalyzer(true)
                .enableMatch(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.SparseFloatVector)
                .build());

        schema.addFunction(Function.builder()
                .functionType(FunctionType.BM25)
                .name("function_bm25")
                .inputFieldNames(Collections.singletonList(TITLE_FIELD))
                .outputFieldNames(Collections.singletonList(VECTOR_FIELD))
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection created: " + COLLECTION_NAME);
        System.out.println("BM25 sparse vector is generated from field: " + TITLE_FIELD);
        System.out.println("Highlighting is configured on field: " + TEXT_FIELD);
    }

    private static void insertData(MilvusClientV2 client) {
        Gson gson = new Gson();
        List<JsonObject> rows = Arrays.asList(
                buildRow(gson, 0, "Milvus for scale", "Milvus is an open-source vector database built for scale. This paragraph is intentionally long so the keyword search appears much later in the same text fragment. Search is a core capability for information retrieval systems."),
                buildRow(gson, 1, "Full text search", "Milvus supports full text search with analyzers and BM25. This sentence adds enough spacing and extra wording to separate the two highlighted terms into different regions for the lexical highlighter example."),
                buildRow(gson, 2, "RAG systems", "Vector databases help retrieval augmented generation systems."),
                buildRow(gson, 3, "Milvus users", "This example demonstrates highlighted snippets for modern applications. The word search is placed here with a lot of filler text before Milvus appears again near the end of the document to encourage multiple fragments in highlighter output for Milvus users.")
        );

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        System.out.println("Inserted " + rows.size() + " rows");

        client.flush(FlushReq.builder().collectionNames(Collections.singletonList(COLLECTION_NAME)).build());
    }

    private static JsonObject buildRow(Gson gson, long id, String title, String text) {
        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, id);
        row.addProperty(TITLE_FIELD, title);
        row.addProperty(TEXT_FIELD, text);
        return row;
    }

    private static void searchWithHighlighter(MilvusClientV2 client, List<String> queryTexts) {
        LexicalHighlighter.LexicalHighlighterBuilder highlighterBuilder = LexicalHighlighter.builder()
//                .highlightSearchText(true)
                .addPreTag("<em>")
                .addPostTag("</em>")
                .fragmentSize(40)
                .numOfFragments(10);
        for (String queryText : queryTexts) {
            highlighterBuilder.addHighlightQuery(new LexicalHighlighter.HighlightQuery("TextMatch", TEXT_FIELD, queryText));
        }
        LexicalHighlighter highlighter = highlighterBuilder.build();

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(queryTexts.stream().map(EmbeddedText::new).collect(java.util.stream.Collectors.toList()))
                .annsField(VECTOR_FIELD)
                .filter(String.format("TEXT_MATCH(%s, \"%s\")", TEXT_FIELD, String.join(" ", queryTexts)))
                .limit(3)
                .metricType(IndexParam.MetricType.BM25)
                .outputFields(Arrays.asList(TITLE_FIELD, TEXT_FIELD))
                .highlighter(highlighter)
                .build());

        System.out.println("\nSearch with lexical highlighter: " + queryTexts);
        System.out.println("  Ranking source: BM25 sparse vector generated from field '" + TITLE_FIELD + "'");
        System.out.println("  Highlight source: TEXT_MATCH on field '" + TEXT_FIELD + "'");
        System.out.println("  Output fields: " + Arrays.asList(TITLE_FIELD, TEXT_FIELD));
        for (List<SearchResp.SearchResult> results : searchResp.getSearchResults()) {
            System.out.println("\n-----------------------------------------------------------------------------");
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
                printHighlightResult(result.getHighlightResult(TEXT_FIELD));
                printHighlightResult(result.getHighlightResult(TITLE_FIELD));
                Map<String, Object> entity = result.getEntity();
                if (entity != null) {
                    System.out.println("  title: " + entity.get(TITLE_FIELD));
                    System.out.println("  text: " + entity.get(TEXT_FIELD));
                }
            }
        }
        System.out.println("=============================================================");
    }

    private static void printHighlightResult(SearchResp.HighlightResult highlightResult) {
        if (highlightResult == null) {
            return;
        }
        System.out.println("  highlighted field: " + highlightResult.getFieldName());
        System.out.println("  fragments: " + highlightResult.getFragments());
        System.out.println("  scores: " + highlightResult.getScores());
    }

    public static void main(String[] args) {
        MilvusClientV2 client = connect();
        try {
            createCollection(client);
            insertData(client);
            searchWithHighlighter(client, Arrays.asList("milvus users", "text search"));
        } finally {
            client.close();
        }
    }
}

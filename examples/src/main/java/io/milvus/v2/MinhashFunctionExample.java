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

import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MinhashFunctionExample {
    private static final String COLLECTION_NAME = "java_sdk_example_minhash_v2";
    private static final String ID_FIELD = "id";
    private static final String TEXT_FIELD = "text";
    private static final String SIGNATURE_FIELD = "minhash_signature";
    private static final int NUM_HASHES = 16;
    private static final int SIGNATURE_DIM = NUM_HASHES * 32;

    private static void createDedupCollection(MilvusClientV2 client) {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

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
                .name("text_to_minhash")
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
                .extraParams(new java.util.HashMap<String, Object>() {{
                    put("mh_lsh_band", 8);
                    put("with_raw_data", true);
                }})
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        System.out.println("Collection created");
    }

    private static List<String> insertTexts(MilvusClientV2 client) {
        List<String> texts = Arrays.asList(
                "The quick brown fox jumps over the lazy dog.",
                "A quick brown fox jumped over a lazy dog.",
                "The fast brown fox leaps over the sleepy dog.",
                "Machine learning is transforming artificial intelligence.",
                "Deep learning transforms artificial intelligence research.",
                "Completely unrelated text about cooking recipes.",
                "Completely unrelated text about cooking recipes!"
        );

        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i + 1L);
            row.addProperty(TEXT_FIELD, texts.get(i));
            rows.add(row);
        }

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        QueryResp countResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows in collection%n", (long) countResp.getQueryResults().get(0).getEntity().get("count(*)"));
        return texts;
    }

    private static SearchResp searchByText(MilvusClientV2 client, String text, int topK) {
        return client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(SIGNATURE_FIELD)
                .data(Collections.singletonList(new EmbeddedText(text)))
                .metricType(IndexParam.MetricType.MHJACCARD)
                .searchParams(new java.util.HashMap<String, Object>() {{
                    put("mh_search_with_jaccard", true);
                    put("refine_k", 50);
                }})
                .limit(topK)
                .outputFields(Arrays.asList(ID_FIELD, TEXT_FIELD))
                .build());
    }

    private static void deduplicateTexts(MilvusClientV2 client, List<String> texts, double similarityThreshold, int topK) {
        Set<Long> uniqueIds = new HashSet<>();
        List<String> duplicates = new ArrayList<>();

        for (int i = 0; i < texts.size(); i++) {
            long docId = i + 1L;
            String text = texts.get(i);
            SearchResp searchResp = searchByText(client, text, topK);
            List<SearchResp.SearchResult> hits = searchResp.getSearchResults().get(0);

            boolean isDuplicate = false;
            for (SearchResp.SearchResult hit : hits) {
                long hitId = ((Number) hit.getEntity().get(ID_FIELD)).longValue();
                if (hitId == docId) {
                    continue;
                }
                if (hit.getScore() >= similarityThreshold && hitId < docId) {
                    duplicates.add(String.format("ID %d is duplicate of ID %d, similarity=%.4f", docId, hitId, hit.getScore()));
                    isDuplicate = true;
                    break;
                }
            }

            if (!isDuplicate) {
                uniqueIds.add(docId);
            }
        }

        System.out.println("\nUnique texts:");
        for (int i = 0; i < texts.size(); i++) {
            long id = i + 1L;
            if (uniqueIds.contains(id)) {
                System.out.println("  - " + texts.get(i));
            }
        }

        System.out.println("\nDuplicates:");
        if (duplicates.isEmpty()) {
            System.out.println("  (none)");
        } else {
            for (String duplicate : duplicates) {
                System.out.println("  - " + duplicate);
            }
        }
    }

    public static void main(String[] args) {
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());

        createDedupCollection(client);
        List<String> texts = insertTexts(client);
        deduplicateTexts(client, texts, 0.8, 5);

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        client.close();
    }
}

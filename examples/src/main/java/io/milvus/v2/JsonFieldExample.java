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
import io.milvus.v1.CommonUtils;
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
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class JsonFieldExample {
    private static final String COLLECTION_NAME = "java_sdk_example_json_v2";
    private static final String ID_FIELD = "key";
    private static final String VECTOR_FIELD = "vector";
    private static final String JSON_FIELD = "metadata";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClientV2 client, String expr) {
        System.out.printf("%n=============================Query with expr: '%s'================================%n", expr);
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(expr)
                .outputFields(Arrays.asList(ID_FIELD, JSON_FIELD, "dynamic1", "dynamic2"))
                .build());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResp.QueryResult> records = queryRet.getQueryResults();
        for (QueryResp.QueryResult record : records) {
            System.out.println(record.getEntity());
        }
    }

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(JSON_FIELD)
                .dataType(DataType.JSON)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        // Create INVERTED index for a specific entry of JSON field
        // Index for JSON field is supported from milvus v2.5.7 and fully supported in v2.5.13+
        // Read the doc for more info: https://milvus.io/docs/json-indexing.md
        Map<String,Object> p1 = new HashMap<>();
        p1.put("json_path", "metadata[\"flags\"]");
        p1.put("json_cast_type", "array_double");
        indexes.add(IndexParam.builder()
                .fieldName(JSON_FIELD)
                .indexType(IndexParam.IndexType.INVERTED)
                .extraParams(p1)
                .build());

        // Create NGRAM index for a specific entry of JSON field
        // NGRAM index for JSON field is supported from milvus v2.6.2
        // Read the doc for more info: https://milvus.io/docs/ngram.md
        Map<String,Object> p2 = new HashMap<>();
        p2.put("json_path","metadata[\"path\"]");
        p2.put("json_cast_type", "varchar");
        p2.put("min_gram", 3);
        p2.put("max_gram", 5);
        indexes.add(IndexParam.builder()
                .fieldName(JSON_FIELD)
                .indexType(IndexParam.IndexType.NGRAM)
                .extraParams(p2)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert rows
        List<List<Float>> vectors = new ArrayList<>();
        List<JsonObject> metadatas = new ArrayList<>();
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            List<Float> vector = CommonUtils.generateFloatVector(VECTOR_DIM);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector));
            vectors.add(vector);

            // Note: for JSON field, always construct a real JsonObject
            // don't use row.addProperty(JSON_FIELD, strContent) since the value is treated as a string, not a JsonObject
            JsonObject metadata = new JsonObject();
            metadata.addProperty("path", String.format("\\root/abc_%d/path_%d", i, i));
            metadata.addProperty("size", i);
            if (i%7 == 0) {
                metadata.addProperty("special", true);
            }
            metadata.add("flags", gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.add(JSON_FIELD, metadata);
            metadatas.add(metadata);
//            System.out.println(metadata);

            // dynamic fields
            if (i%2 == 0) {
                row.addProperty("dynamic1", (double)i/3);
            } else {
                row.addProperty("dynamic2", "ok");
            }

            client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(row))
                    .build());
        }

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // Search and output JSON field
        List<BaseVector> searchVectors = new ArrayList<>();
        List<JsonObject> expectedMetadatas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Float> targetVector = vectors.get(i);
            searchVectors.add(new FloatVec(targetVector));
            expectedMetadatas.add(metadatas.get(i));
        }
        SearchResp searchRet = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(searchVectors)
                .limit(3L)
                .annsField(VECTOR_FIELD)
                .outputFields(Arrays.asList(ID_FIELD, VECTOR_FIELD, JSON_FIELD))
                .build());

        System.out.println("\n=============================Search result================================");
        List<List<SearchResp.SearchResult>> searchResults = searchRet.getSearchResults();
        for (int i = 0; i < 10; i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            System.out.printf("\nThe result of No.%d target vector:\n", i);
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }

            long pk = (long)results.get(0).getId();
            if (pk != i) {
                throw new RuntimeException(String.format("The top1 ID %d is not equal to target vector's ID %d", pk, i));
            }
            JsonObject metadata = (JsonObject) results.get(0).getEntity().get(JSON_FIELD);
            if (!metadata.equals(expectedMetadatas.get(i))) {
                throw new RuntimeException(String.format("The top1 metadata %s is not equal to target metadata %s",
                        metadata, expectedMetadatas.get(i)));
            }
            List<Float> vector = (List<Float>) results.get(0).getEntity().get(VECTOR_FIELD);
            CommonUtils.compareFloatVectors(vector, (List<Float>)searchVectors.get(i).getData());
        }

        // Query by filtering JSON
        queryWithExpr(client, "exists metadata[\"special\"]");
        queryWithExpr(client, "metadata[\"size\"] < 5");
        queryWithExpr(client, "metadata[\"size\"] in [4, 5, 6]");
        queryWithExpr(client, "JSON_CONTAINS(metadata[\"flags\"], 9)");
        queryWithExpr(client, "JSON_CONTAINS_ANY(metadata[\"flags\"], [8, 9, 10])");
        queryWithExpr(client, "JSON_CONTAINS_ALL(metadata[\"flags\"], [8, 9, 10])");
        queryWithExpr(client, "metadata[\"path\"] LIKE \"%c_5%\"");
        queryWithExpr(client, "dynamic1 < 2.0");

        client.close();
    }
}

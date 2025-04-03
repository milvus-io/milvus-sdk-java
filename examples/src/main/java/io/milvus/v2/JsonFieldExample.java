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
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;

public class JsonFieldExample {
    private static final String COLLECTION_NAME = "java_sdk_example_json_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final String JSON_FIELD = "metadata";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClientV2 client, String expr) {
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
        System.out.println("=============================================================");
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

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert rows
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            // Note: for JSON field, always construct a real JsonObject
            // don't use row.addProperty(JSON_FIELD, strContent) since the value is treated as a string, not a JsonObject
            JsonObject metadata = new JsonObject();
            metadata.addProperty("path", String.format("\\root/abc/path%d", i));
            metadata.addProperty("size", i);
            if (i%7 == 0) {
                metadata.addProperty("special", true);
            }
            metadata.add("flags", gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.add(JSON_FIELD, metadata);
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
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // Query by filtering JSON
        queryWithExpr(client, "exists metadata[\"special\"]");
        queryWithExpr(client, "metadata[\"size\"] < 5");
        queryWithExpr(client, "metadata[\"size\"] in [4, 5, 6]");
        queryWithExpr(client, "JSON_CONTAINS(metadata[\"flags\"], 9)");
        queryWithExpr(client, "JSON_CONTAINS_ANY(metadata[\"flags\"], [8, 9, 10])");
        queryWithExpr(client, "JSON_CONTAINS_ALL(metadata[\"flags\"], [8, 9, 10])");
        queryWithExpr(client, "dynamic1 < 2.0");

        client.close();
    }
}

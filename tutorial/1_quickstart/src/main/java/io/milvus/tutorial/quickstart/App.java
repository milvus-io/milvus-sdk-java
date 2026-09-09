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

package io.milvus.tutorial.quickstart;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tutorial 1: Quick start.
 *
 * <p>The smallest complete MilvusClientV2 application. It connects to Milvus, creates a collection
 * with an integer primary key, a text field, and a float vector, inserts two rows, performs a
 * vector search, and removes the collection.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            String collectionName = "tutorial_quickstart";

            // Drop the collection if it already exists from a previous run.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            // Define the schema: an integer primary key, a text field, and a 4-dim float vector.
            CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
            schema.addField(AddFieldReq.builder()
                    .fieldName("id")
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("text")
                    .dataType(DataType.VarChar)
                    .maxLength(256)
                    .build());
            schema.addField(AddFieldReq.builder()
                    .fieldName("vector")
                    .dataType(DataType.FloatVector)
                    .dimension(4)
                    .build());

            client.createCollection(CreateCollectionReq.builder()
                    .collectionName(collectionName)
                    .collectionSchema(schema)
                    .build());
            System.out.printf("Collection '%s' created%n", collectionName);

            // Create an index on the vector field. A collection must be indexed before it can be
            // loaded into memory for search.
            client.createIndex(CreateIndexReq.builder()
                    .collectionName(collectionName)
                    .indexParams(Collections.singletonList(IndexParam.builder()
                            .fieldName("vector")
                            .indexType(IndexParam.IndexType.AUTOINDEX)
                            .build()))
                    .build());

            // Insert two rows.
            Gson gson = new Gson();
            List<JsonObject> rows = new ArrayList<>();
            for (int i = 1; i <= 2; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("id", i);
                row.addProperty("text", "row " + i);
                row.add("vector", gson.toJsonTree(new float[]{1.0f * i, 0.5f * i, 0.25f * i, 0.125f * i}));
                rows.add(row);
            }
            InsertResp insertResp = client.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(rows)
                    .build());
            System.out.printf("%d rows inserted%n", insertResp.getInsertCnt());

            // Load the collection into memory so search can read the data.
            client.loadCollection(LoadCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            // Search for the nearest neighbors of a query vector.
            SearchResp searchResp = client.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(new FloatVec(new float[]{1.0f, 0.5f, 0.25f, 0.125f})))
                    .limit(2)
                    .build());
            System.out.println("Search results:");
            for (List<SearchResp.SearchResult> results : searchResp.getSearchResults()) {
                for (SearchResp.SearchResult result : results) {
                    System.out.printf("  id=%s score=%f%n", result.getId(), result.getScore());
                }
            }

            // Clean up.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.printf("Collection '%s' dropped%n", collectionName);
        } finally {
            client.close();
        }
    }
}

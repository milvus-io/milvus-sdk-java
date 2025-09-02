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

import com.google.gson.*;
import io.milvus.v2.client.*;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.*;

import java.util.*;

public class SimpleExample {
    public static void main(String[] args) {

        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        String collectionName = "java_sdk_example_simple_v2";
        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Quickly create a collection with "id" field and "vector" field
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .dimension(4)
                .build());
        System.out.printf("Collection '%s' created\n", collectionName);

        // Insert some data
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("vector", gson.toJsonTree(new float[]{i, (float) i /2, (float) i /3, (float) i /4}));
            row.addProperty(String.format("dynamic_%d", i), "this is dynamic value"); // this value is stored in dynamic field
            rows.add(row);
        }
        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(rows)
                .build());
        System.out.printf("%d rows inserted\n", insertR.getInsertCnt());

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // Retrieve
        List<Object> ids = Arrays.asList(1L, 50L);
        GetResp getR = client.get(GetReq.builder()
                .collectionName(collectionName)
                .ids(ids)
                .outputFields(Collections.singletonList("*"))
                .build());
        System.out.println("\nRetrieve results:");
        for (QueryResp.QueryResult result : getR.getGetResults()) {
            System.out.println(result.getEntity());
        }

        // Search
        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(new FloatVec(new float[]{1.0f, 1.0f, 1.0f, 1.0f})))
                .filter("id < 100")
                .limit(10)
                .outputFields(Collections.singletonList("*"))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchR.getSearchResults();
        System.out.println("\nSearch results:");
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("ID: %d, Score: %f, %s\n", (long)result.getId(), result.getScore(), result.getEntity().toString());
            }
        }

        // search with template expression
        Map<String, Map<String, Object>> expressionTemplateValues = new HashMap<>();
        Map<String, Object> params = new HashMap<>();
        params.put("max", 10);
        expressionTemplateValues.put("id < {max}", params);

        List<Object> list = Arrays.asList(1, 2, 3);
        Map<String, Object> params2 = new HashMap<>();
        params2.put("list", list);
        expressionTemplateValues.put("id in {list}", params2);

        expressionTemplateValues.forEach((key, value) -> {
            SearchReq request = SearchReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(new FloatVec(new float[]{1.0f, 1.0f, 1.0f, 1.0f})))
                    .limit(10)
                    .filter(key)
                    .filterTemplateValues(value)
                    .outputFields(Collections.singletonList("*"))
                    .build();
            SearchResp statusR = client.search(request);
            List<List<SearchResp.SearchResult>> searchResults2 = statusR.getSearchResults();
            System.out.println("\nSearch with template results:");
            for (List<SearchResp.SearchResult> results : searchResults2) {
                for (SearchResp.SearchResult result : results) {
                    System.out.printf("ID: %d, Score: %f, %s\n", (long)result.getId(), result.getScore(), result.getEntity().toString());
                }
            }
        });

        client.close();
    }
}

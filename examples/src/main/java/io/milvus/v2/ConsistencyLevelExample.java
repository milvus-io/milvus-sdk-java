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
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class ConsistencyLevelExample {
    private static final MilvusClientV2 client;

    static {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        client = new MilvusClientV2(config);
    }

    private static final String COLLECTION_NAME_PREFIX = "java_sdk_example_clevel_v2_";
    private static final Integer VECTOR_DIM = 512;

    private static String createCollection(ConsistencyLevel level) {
        String collectionName = COLLECTION_NAME_PREFIX + level.getName();

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Quickly create a collection with "id" field and "vector" field
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .dimension(VECTOR_DIM)
                .consistencyLevel(level)
                .build());
        System.out.printf("Collection '%s' created\n", collectionName);
        return collectionName;
    }

    private static void showCollectionLevel(String collectionName) {
        DescribeCollectionResp resp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.printf("Default consistency level: %s\n", resp.getConsistencyLevel().getName());
    }

    private static int insertData(String collectionName) {
        Gson gson = new Gson();
        int rowCount = 1000;
        for (int i = 0; i < rowCount; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("vector", gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            client.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(row))
                    .build());
        }

        System.out.printf("%d rows inserted\n", rowCount);
        return rowCount;
    }

    private static List<SearchResp.SearchResult> search(String collectionName, int topK) {
        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .limit(topK)
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchR.getSearchResults();
        return searchResults.get(0);
    }

    private static void testStrongLevel() {
        String collectionName = createCollection(ConsistencyLevel.STRONG);
        showCollectionLevel(collectionName);
        int rowCount = insertData(collectionName);

        // immediately search after insert, for Strong level, all the entities are visible
        List<SearchResp.SearchResult> results = search(collectionName, rowCount);
        if (results.size() != rowCount) {
            throw new RuntimeException(String.format("All inserted entities should be visible with Strong" +
                    " consistency level, but only %d returned", results.size()));
        }
        System.out.printf("Strong level is working fine, %d results returned\n", results.size());
    }

    private static void testSessionLevel() throws Exception {
        String collectionName = createCollection(ConsistencyLevel.SESSION);
        showCollectionLevel(collectionName);

        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        PoolConfig poolConfig = PoolConfig.builder()
                .maxIdlePerKey(10) // max idle clients per key
                .maxTotalPerKey(20) // max total(idle + active) clients per key
                .maxTotal(100) // max total clients for all keys
                .maxBlockWaitDuration(Duration.ofSeconds(5L)) // getClient() will wait 5 seconds if no idle client available
                .minEvictableIdleDuration(Duration.ofSeconds(10L)) // if number of idle clients is larger than maxIdlePerKey, redundant idle clients will be evicted after 10 seconds
                .build();
        MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig);

        // The same process, different MilvusClient object, insert and search with Session level.
        // The Session level ensure that the newly inserted data instantaneously become searchable.
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            List<Float> vector = CommonUtils.generateFloatVector(VECTOR_DIM);
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("vector", gson.toJsonTree(vector));

            // insert by a MilvusClient
            String clientName1 = String.format("client_%d", i%10);
            MilvusClientV2 client1 = pool.getClient(clientName1);
            client1.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(row))
                    .build());
            pool.returnClient(clientName1, client1); // don't forget to return the client to pool
            System.out.println("insert");

            // search by another MilvusClient, use the just inserted vector to search
            // the returned item is expected to be the just inserted item
            String clientName2 = String.format("client_%d", i%10+1);
            MilvusClientV2 client2 = pool.getClient(clientName2);
            SearchResp searchR = client2.search(SearchReq.builder()
                    .collectionName(collectionName)
                    .data(Collections.singletonList(new FloatVec(vector)))
                    .limit(1)
                    .build());
            pool.returnClient(clientName2, client2); // don't forget to return the client to pool
            List<List<SearchResp.SearchResult>> searchResults = searchR.getSearchResults();
            List<SearchResp.SearchResult> results = searchResults.get(0);
            if (results.size() != 1) {
                throw new RuntimeException("Search result is empty");
            }
            if (i != (Long)results.get(0).getId()) {
                throw new RuntimeException("The just inserted entity is not found");
            }
            System.out.println("search");
        }

        System.out.println("Session level is working fine");
    }

    private static void testBoundedLevel() {
        String collectionName = createCollection(ConsistencyLevel.BOUNDED);
        showCollectionLevel(collectionName);
        int rowCount = insertData(collectionName);

        // immediately search after insert, for Bounded level, not all the entities are visible
        List<SearchResp.SearchResult> results = search(collectionName, rowCount);
        System.out.printf("Bounded level is working fine, %d results returned\n", results.size());
    }

    private static void testEventuallyLevel() {
        String collectionName = createCollection(ConsistencyLevel.EVENTUALLY);
        showCollectionLevel(collectionName);
        int rowCount = insertData(collectionName);

        // immediately search after insert, for Bounded level, not all the entities are visible
        List<SearchResp.SearchResult> results = search(collectionName, rowCount);
        System.out.printf("Eventually level is working fine, %d results returned\n", results.size());
    }

    public static void main(String[] args) throws Exception {
        testStrongLevel();
        System.out.println("==============================================================");
        testSessionLevel();
        System.out.println("==============================================================");
        testBoundedLevel();
        System.out.println("==============================================================");
        testEventuallyLevel();
    }
}

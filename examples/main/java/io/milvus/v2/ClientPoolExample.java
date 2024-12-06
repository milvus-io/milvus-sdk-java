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
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ClientPoolExample {
    public static String CollectionName = "java_sdk_example_pool_v2";
    public static String VectorFieldName = "vector";
    public static int DIM = 128;

    public static void createCollection(MilvusClientV2Pool pool) {
        String tempKey = "temp";
        MilvusClientV2 client = pool.getClient(tempKey);
        if (client == null) {
            throw new RuntimeException("Unable to create client");
        }
        try {
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(CollectionName)
                    .build());
            client.createCollection(CreateCollectionReq.builder()
                    .collectionName(CollectionName)
                    .primaryFieldName("id")
                    .idType(DataType.Int64)
                    .autoID(Boolean.TRUE)
                    .vectorFieldName(VectorFieldName)
                    .dimension(DIM)
                    .build());
            System.out.printf("Collection '%s' created%n", CollectionName);
        } catch (Exception e) {
            String msg = String.format("Failed to create collection, error: %s%n", e.getMessage());
            System.out.printf(msg);
            throw new RuntimeException(msg);
        } finally {
            pool.returnClient(tempKey, client);
            pool.clear(tempKey);
        }
    }

    public static Thread runInsertThread(MilvusClientV2Pool pool, String clientName, int repeatRequests) {
        Thread t = new Thread(() -> {
            Gson gson = new Gson();
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClientV2 client = null;
                while(client == null) {
                    try {
                        // getClient() might exceeds the borrowMaxWaitMillis and throw exception
                        // retry to call until it return a client
                        client = pool.getClient(clientName);
                    } catch (Exception e) {
                        System.out.printf("Failed to get client, will retry, error: %s%n", e.getMessage());
                    }
                }
                try {
                    int rowCount = 1;
                    List<JsonObject> rows = new ArrayList<>();
                    for (int j = 0; j < rowCount; j++) {
                        JsonObject row = new JsonObject();
                        row.add(VectorFieldName, gson.toJsonTree(CommonUtils.generateFloatVector(DIM)));
                        rows.add(row);
                    }
                    InsertResp insertR = client.insert(InsertReq.builder()
                            .collectionName(CollectionName)
                            .data(rows)
                            .build());
                    System.out.printf("%d rows inserted%n", rows.size());
                } catch (Exception e) {
                    System.out.printf("Failed to inserted, error: %s%n", e.getMessage());
                } finally {
                    pool.returnClient(clientName, client); // make sure the client is returned after use
                }
            }
            System.out.printf("Insert thread %s finished%n", Thread.currentThread().getName());
        });
        t.start();
        return t;
    }

    public static Thread runSearchThread(MilvusClientV2Pool pool, String clientName, int repeatRequests) {
        Thread t = new Thread(() -> {
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClientV2 client = null;
                while(client == null) {
                    try {
                        // getClient() might exceeds the borrowMaxWaitMillis and throw exception
                        // retry to call until it return a client
                        client = pool.getClient(clientName);
                    } catch (Exception e) {
                        System.out.printf("Failed to get client, will retry, error: %s%n", e.getMessage());
                    }
                }
                try {
                    SearchResp result = client.search(SearchReq.builder()
                            .collectionName(CollectionName)
                            .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                            .annsField(VectorFieldName)
                            .topK(10)
                            .data(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(DIM))))
                            .build());
                    System.out.println("A search request completed");
                } catch (Exception e) {
                    System.out.printf("Failed to search, error: %s%n", e.getMessage());
                } finally {
                    pool.returnClient(clientName, client); // make sure the client is returned after use
                }
            }
            System.out.printf("Search thread %s finished%n", Thread.currentThread().getName());
        });
        t.start();
        return t;
    }

    public static void main(String[] args) throws InterruptedException {
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
        MilvusClientV2Pool pool;
        try {
            pool = new MilvusClientV2Pool(poolConfig, connectConfig);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        createCollection(pool);

        List<Thread> threadList = new ArrayList<>();
        int threadCount = 100;
        int repeatRequests = 100;
        long start = System.currentTimeMillis();
        for (int k = 0; k < threadCount; k++) {
            threadList.add(runInsertThread(pool, "192.168.1.1", repeatRequests));
            threadList.add(runInsertThread(pool, "192.168.1.2", repeatRequests));
            threadList.add(runInsertThread(pool, "192.168.1.3", repeatRequests));

            threadList.add(runSearchThread(pool, "192.168.1.1", repeatRequests));
            threadList.add(runSearchThread(pool, "192.168.1.2", repeatRequests));
            threadList.add(runSearchThread(pool, "192.168.1.3", repeatRequests));

            System.out.printf("Total %d idle clients and %d active clients%n",
                    pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());
        }

        for (Thread t : threadList) {
            t.join();
        }
        long end = System.currentTimeMillis();
        System.out.printf("%d insert requests and %d search requests finished in %.3f seconds%n",
                threadCount*repeatRequests*3, threadCount*repeatRequests*3, (end-start)*0.001);
        System.out.printf("Total %d idle clients and %d active clients%n",
                pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());

        pool.clear(); // clear idle clients
        System.out.printf("After clear, total %d idle clients and %d active clients%n",
                pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());

        pool.close();
    }
}

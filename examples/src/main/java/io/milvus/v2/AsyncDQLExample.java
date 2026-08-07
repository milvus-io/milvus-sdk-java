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
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class AsyncDQLExample {
    private static final String COLLECTION_NAME = "java_sdk_example_async_dql_v2";
    private static final int DEFAULT_REQUEST_ROUNDS = 1000;
    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 20;

    public static void main(String[] args) {
        int requestRounds = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_REQUEST_ROUNDS;
        int maxConcurrentRequests = args.length > 1
                ? Integer.parseInt(args[1]) : DEFAULT_MAX_CONCURRENT_REQUESTS;
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());

        try {
            prepareCollection(client);
            runAsyncDql(client, requestRounds, maxConcurrentRequests);
        } finally {
            try {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .build());
            } finally {
                client.close();
            }
        }
    }

    private static void prepareCollection(MilvusClientV2 client) {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .dimension(4)
                .build());

        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.addProperty("name", "item-" + i);
            row.add("vector", gson.toJsonTree(
                    new float[]{i, (float) i / 2, (float) i / 3, (float) i / 4}));
            rows.add(row);
        }
        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        System.out.println("Collection prepared with 100 rows\n");
    }

    private static void runAsyncDql(MilvusClientV2 client, int requestRounds,
                                    int maxConcurrentRequests) {
        if (requestRounds <= 0 || maxConcurrentRequests <= 0) {
            throw new IllegalArgumentException("Request rounds and concurrency must be greater than zero");
        }

        Semaphore concurrencyLimit = new Semaphore(maxConcurrentRequests);
        List<CompletableFuture<?>> futures = new ArrayList<>(requestRounds * 3);
        AtomicInteger queryResultCount = new AtomicInteger();
        AtomicInteger searchResultCount = new AtomicInteger();
        AtomicInteger hybridSearchResultCount = new AtomicInteger();
        long startNanos = System.nanoTime();
        System.out.printf("Submitting %d rounds (%d total requests), max concurrency=%d%n",
                requestRounds, requestRounds * 3, maxConcurrentRequests);

        for (int i = 0; i < requestRounds; i++) {
            int requestId = i;
            futures.add(submitWithLimit(concurrencyLimit, () -> client.queryAsync(QueryReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .filter(String.format("id >= %d and id < %d", requestId % 95, requestId % 95 + 5))
                    .outputFields(Arrays.asList("id", "name"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build())).thenAccept(response ->
                    queryResultCount.addAndGet(response.getQueryResults().size())));

            float value = requestId % 100 + 1;
            futures.add(submitWithLimit(concurrencyLimit, () -> client.searchAsync(SearchReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(new FloatVec(
                            new float[]{value, value / 2, value / 3, value / 4})))
                    .limit(5)
                    .outputFields(Arrays.asList("id", "name"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build())).thenAccept(response ->
                    searchResultCount.addAndGet(countSearchResults(response))));

            futures.add(submitWithLimit(concurrencyLimit, () -> client.hybridSearchAsync(
                    buildHybridSearchRequest(value))).thenAccept(response ->
                    hybridSearchResultCount.addAndGet(countSearchResults(response))));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
        System.out.printf("Completed %d async requests with max concurrency %d in %d ms%n",
                futures.size(), maxConcurrentRequests, elapsedMs);
        System.out.printf("Returned entities: query=%d, search=%d, hybridSearch=%d%n",
                queryResultCount.get(), searchResultCount.get(), hybridSearchResultCount.get());
    }

    private static HybridSearchReq buildHybridSearchRequest(float value) {
        List<AnnSearchReq> annSearchRequests = Arrays.asList(
                AnnSearchReq.builder()
                        .vectorFieldName("vector")
                        .vectors(Collections.singletonList(
                                new FloatVec(new float[]{value, value / 2, value / 3, value / 4})))
                        .filter("id < 50")
                        .limit(10)
                        .build(),
                AnnSearchReq.builder()
                        .vectorFieldName("vector")
                        .vectors(Collections.singletonList(
                                new FloatVec(new float[]{value + 1, value, value / 2, value / 3})))
                        .filter("id >= 50")
                        .limit(10)
                        .build());
        return HybridSearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .searchRequests(annSearchRequests)
                .functionScore(FunctionScore.builder()
                        .addFunction(RRFRanker.builder().k(60).build())
                        .build())
                .limit(5)
                .outFields(Arrays.asList("id", "name"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();
    }

    private static int countSearchResults(SearchResp response) {
        return response.getSearchResults().stream().mapToInt(List::size).sum();
    }

    private static <T> CompletableFuture<T> submitWithLimit(
            Semaphore concurrencyLimit, Supplier<CompletableFuture<T>> request) {
        concurrencyLimit.acquireUninterruptibly();
        try {
            CompletableFuture<T> future = request.get();
            future.whenComplete((response, throwable) -> concurrencyLimit.release());
            return future;
        } catch (RuntimeException | Error throwable) {
            concurrencyLimit.release();
            throw throwable;
        }
    }
}

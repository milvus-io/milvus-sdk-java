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

package io.milvus.v2.client.globalcluster;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.SearchResults;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.utils.RpcUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class GlobalClusterCacheTest {
    @AfterEach
    void clearCache() {
        CollectionTsCache.getInstance().clear();
    }

    @Test
    void primaryChangePreservesLogicalGlobalEndpointForSessionTimestamp() throws Exception {
        String logicalEndpoint = "global.example.com:443";
        MilvusClientV2 globalClient = new MilvusClientV2(null);
        setField(globalClient, "connectConfig", ConnectConfig.builder()
                .uri("https://global.example.com:443")
                .dbName("db")
                .build());
        setField(globalClient, "cacheEndpoint", logicalEndpoint);

        MilvusClientV2 newPrimary = new MilvusClientV2(null);
        setField(newPrimary, "cacheEndpoint", "primary-b.example.com:443");
        CollectionTsCache.getInstance().set(logicalEndpoint, "db", "coll", 100L);

        Method updatePrimary = MilvusClientV2.class.getDeclaredMethod("updatePrimaryConnection", MilvusClientV2.class);
        updatePrimary.setAccessible(true);
        updatePrimary.invoke(globalClient, newPrimary);

        assertEquals(logicalEndpoint, getField(globalClient, "cacheEndpoint"));
        VectorService vectorService = (VectorService) getField(globalClient, "vectorService");
        QueryRequest query = vectorService.vectorUtils.ConvertToGrpcQueryRequest(QueryReq.builder()
                .collectionName("coll")
                .consistencyLevel(ConsistencyLevel.SESSION)
                .build());
        assertEquals(100L, query.getGuaranteeTimestamp());
    }

    @Test
    void asyncRetryUsesReplacementFutureStubAfterPrimaryChange() throws Exception {
        MilvusServiceGrpc.MilvusServiceFutureStub oldFutureStub =
                Mockito.mock(MilvusServiceGrpc.MilvusServiceFutureStub.class);
        MilvusServiceGrpc.MilvusServiceFutureStub newFutureStub =
                Mockito.mock(MilvusServiceGrpc.MilvusServiceFutureStub.class);
        when(oldFutureStub.withOption(any(), any())).thenReturn(oldFutureStub);
        when(newFutureStub.withOption(any(), any())).thenReturn(newFutureStub);
        SettableFuture<SearchResults> oldAttempt = SettableFuture.create();
        when(oldFutureStub.search(any(SearchRequest.class))).thenReturn(oldAttempt);
        when(newFutureStub.search(any(SearchRequest.class))).thenReturn(
                Futures.immediateFuture(SearchResults.newBuilder().build()));

        ConnectConfig config = ConnectConfig.builder()
                .uri("https://global.example.com:443")
                .build();
        MilvusClientV2 globalClient = new MilvusClientV2(null);
        setField(globalClient, "connectConfig", config);
        setField(globalClient, "futureStub", oldFutureStub);
        globalClient.retryConfig(RetryConfig.builder()
                .maxRetryTimes(2)
                .initialBackOffMs(10)
                .maxBackOffMs(10)
                .backOffMultiplier(1)
                .build());

        MilvusClientV2 newPrimary = new MilvusClientV2(null);
        setField(newPrimary, "futureStub", newFutureStub);
        Method updatePrimary = MilvusClientV2.class.getDeclaredMethod(
                "updatePrimaryConnection", MilvusClientV2.class);
        updatePrimary.setAccessible(true);
        RpcUtils rpcUtils = (RpcUtils) getField(globalClient, "rpcUtils");
        rpcUtils.setGlobalRefreshTrigger(() -> {
            CompletableFuture.runAsync(() -> {
                try {
                    updatePrimary.invoke(globalClient, newPrimary);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            }).join();
        });

        SearchReq request = SearchReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(1)
                .build();
        CompletableFuture<SearchResp> result = globalClient.searchAsync(request);

        verify(oldFutureStub).search(any(SearchRequest.class));
        verify(newFutureStub, never()).search(any(SearchRequest.class));
        oldAttempt.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());

        assertNotNull(result.get(1, TimeUnit.SECONDS));
        verify(newFutureStub).search(any(SearchRequest.class));
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}

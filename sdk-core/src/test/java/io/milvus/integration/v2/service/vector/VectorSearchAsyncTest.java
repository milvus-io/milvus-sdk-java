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

package io.milvus.integration.v2.service.vector;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.milvus.common.interceptor.ClientRequestInterceptor;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class VectorSearchAsyncTest extends BaseTest {

    @Test
    void testSearchAsync() throws Exception {
        SearchReq request = SearchReq.builder()
                .collectionName("test2")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        SearchResp response = client_v2.searchAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertEquals(123L, response.getCost());
        Assertions.assertEquals(456L, response.getScannedRemoteBytes());
        Assertions.assertEquals(789L, response.getScannedTotalBytes());
        Assertions.assertEquals(0.5f, response.getCacheHitRatio());
        verify(futureStub).search(any(SearchRequest.class));
        verify(blockingStub, never()).search(any(SearchRequest.class));
    }

    @Test
    void testSearchAsyncServerFailureCompletesExceptionally() {
        SearchResults failedResponse = SearchResults.newBuilder()
                .setStatus(Status.newBuilder().setCode(1).setReason("search failed").build())
                .build();
        when(futureStub.search(any())).thenReturn(Futures.immediateFuture(failedResponse));
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> client_v2.searchAsync(request).get(1, TimeUnit.SECONDS));

        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.SERVER_ERROR,
                ((MilvusClientException) exception.getCause()).getErrorCode());
    }

    @Test
    void testSearchAsyncCancellationPropagatesToGrpcFuture() {
        SettableFuture<SearchResults> grpcFuture = SettableFuture.create();
        when(futureStub.search(any())).thenReturn(grpcFuture);
        SearchReq request = SearchReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();

        CompletableFuture<SearchResp> future = client_v2.searchAsync(request);
        Assertions.assertTrue(future.cancel(true));

        Assertions.assertTrue(grpcFuture.isCancelled());
    }

    @Test
    void testHybridSearchAsync() throws Exception {
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq request = HybridSearchReq.builder()
                .collectionName("test")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();

        SearchResp response = client_v2.hybridSearchAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertNotNull(response);
        Assertions.assertEquals(123L, response.getCost());
        verify(futureStub).hybridSearch(any(HybridSearchRequest.class));
        verify(blockingStub, never()).hybridSearch(any(HybridSearchRequest.class));
    }

    @Test
    void testAsyncDqlRetriesReusePreparedRpcRequests() throws Exception {
        ThreadLocal<String> requestId = configureClientRequestId();
        client_v2.retryConfig(io.milvus.v2.client.RetryConfig.builder()
                .maxRetryTimes(2)
                .initialBackOffMs(0)
                .maxBackOffMs(0)
                .build());

        SettableFuture<QueryResults> firstQuery = SettableFuture.create();
        when(futureStub.query(any())).thenReturn(firstQuery,
                Futures.immediateFuture(QueryResults.newBuilder().build()));
        QueryReq queryReq = QueryReq.builder()
                .collectionName("query_original")
                .filter("id > 0")
                .build();
        requestId.set("44444444444444444444444444444444");
        CompletableFuture<QueryResp> queryFuture = client_v2.queryAsync(queryReq);
        requestId.set("55555555555555555555555555555555");
        queryReq.setCollectionName("query_mutated");
        firstQuery.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        queryFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(futureStub, times(2)).query(queryCaptor.capture());
        queryCaptor.getAllValues().forEach(rpcRequest ->
                Assertions.assertEquals("query_original", rpcRequest.getCollectionName()));
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                "44444444444444444444444444444444");

        clearInvocations(futureStub);
        SettableFuture<SearchResults> firstSearch = SettableFuture.create();
        when(futureStub.search(any())).thenReturn(firstSearch,
                Futures.immediateFuture(SearchResults.newBuilder().build()));
        SearchReq searchReq = SearchReq.builder()
                .collectionName("search_original")
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        requestId.set("66666666666666666666666666666666");
        CompletableFuture<SearchResp> searchFuture = client_v2.searchAsync(searchReq);
        requestId.set("77777777777777777777777777777777");
        searchReq.setCollectionName("search_mutated");
        firstSearch.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        searchFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<SearchRequest> searchCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(futureStub, times(2)).search(searchCaptor.capture());
        searchCaptor.getAllValues().forEach(rpcRequest ->
                Assertions.assertEquals("search_original", rpcRequest.getCollectionName()));
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                "66666666666666666666666666666666");

        clearInvocations(futureStub);
        SettableFuture<SearchResults> firstHybridSearch = SettableFuture.create();
        when(futureStub.hybridSearch(any())).thenReturn(firstHybridSearch,
                Futures.immediateFuture(SearchResults.newBuilder().build()));
        AnnSearchReq annSearchReq = AnnSearchReq.builder()
                .vectorFieldName("vector")
                .vectors(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(10)
                .build();
        HybridSearchReq hybridSearchReq = HybridSearchReq.builder()
                .collectionName("hybrid_original")
                .searchRequests(Collections.singletonList(annSearchReq))
                .limit(10)
                .build();
        requestId.set("88888888888888888888888888888888");
        CompletableFuture<SearchResp> hybridFuture = client_v2.hybridSearchAsync(hybridSearchReq);
        requestId.set("99999999999999999999999999999999");
        hybridSearchReq.setCollectionName("hybrid_mutated");
        annSearchReq.setVectorFieldName("mutated_vector");
        firstHybridSearch.setException(io.grpc.Status.UNAVAILABLE.asRuntimeException());
        hybridFuture.get(1, TimeUnit.SECONDS);
        ArgumentCaptor<HybridSearchRequest> hybridCaptor = ArgumentCaptor.forClass(HybridSearchRequest.class);
        verify(futureStub, times(2)).hybridSearch(hybridCaptor.capture());
        hybridCaptor.getAllValues().forEach(rpcRequest -> {
            Assertions.assertEquals("hybrid_original", rpcRequest.getCollectionName());
            Assertions.assertEquals("vector",
                    getParam(rpcRequest.getRequests(0).getSearchParamsList(), Constant.VECTOR_FIELD));
        });
        verify(futureStub, times(2)).withOption(
                ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                "88888888888888888888888888888888");
        requestId.remove();
    }

    @Test
    void testHybridSearchRejectsInvalidLimitAndRoundDecimal() {
        MilvusClientException negativeLimit = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.hybridSearch(HybridSearchReq.builder().collectionName("book").limit(-1).build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, negativeLimit.getErrorCode());

        MilvusClientException zeroLimit = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.hybridSearch(HybridSearchReq.builder().collectionName("book").limit(0).build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, zeroLimit.getErrorCode());

        MilvusClientException outOfRange = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.hybridSearch(HybridSearchReq.builder().collectionName("book")
                        .limit(10)
                        .roundDecimal(7).build()));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, outOfRange.getErrorCode());
        Assertions.assertTrue(outOfRange.getMessage().contains("round_decimal"));
    }

    private ThreadLocal<String> configureClientRequestId() throws ReflectiveOperationException {
        ThreadLocal<String> requestId = new ThreadLocal<>();
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .clientRequestId(requestId)
                .build();
        Field connectConfig = MilvusClientV2.class.getDeclaredField("connectConfig");
        connectConfig.setAccessible(true);
        connectConfig.set(client_v2, config);
        return requestId;
    }

    private String getParam(List<KeyValuePair> params, String key) {
        return params.stream()
                .filter(param -> key.equals(param.getKey()))
                .map(KeyValuePair::getValue)
                .findFirst()
                .orElse(null);
    }
}

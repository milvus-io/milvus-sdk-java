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

import com.google.common.util.concurrent.SettableFuture;
import io.milvus.common.interceptor.ClientRequestInterceptor;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class VectorQueryAsyncTest extends BaseTest {

    @Test
    void testQueryAsync() throws Exception {
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .limit(10)
                .build();

        QueryResp response = client_v2.queryAsync(request).get(1, TimeUnit.SECONDS);

        Assertions.assertNotNull(response);
        verify(futureStub).query(any(QueryRequest.class));
        verify(blockingStub, never()).query(any(QueryRequest.class));
    }

    @Test
    void testQueryAsyncWithIdsLoadsSchemaAsynchronously() throws Exception {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .limit(10)
                .build();

        try {
            CompletableFuture<QueryResp> resultFuture = client_v2.queryAsync(request);

            Assertions.assertFalse(resultFuture.isDone());
            verify(futureStub).describeCollection(any(DescribeCollectionRequest.class));
            verify(blockingStub, never()).describeCollection(any(DescribeCollectionRequest.class));
            verify(futureStub, never()).query(any(QueryRequest.class));

            schemaFuture.set(describeCollectionResponse());
            Assertions.assertNotNull(resultFuture.get(1, TimeUnit.SECONDS));
            ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
            verify(futureStub).query(queryCaptor.capture());
            Assertions.assertEquals("id in [1]", queryCaptor.getValue().getExpr());
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncCancellationDuringSchemaLoadCancelsDescribeRpc() {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();

        try {
            CompletableFuture<QueryResp> resultFuture = client_v2.queryAsync(request);

            Assertions.assertTrue(resultFuture.cancel(true));
            Assertions.assertTrue(schemaFuture.isCancelled());
            verify(futureStub, never()).query(any(QueryRequest.class));
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncCancellationPreservesSharedSchemaLoad() throws Exception {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();

        try {
            CompletableFuture<QueryResp> cancelled = client_v2.queryAsync(request);
            CompletableFuture<QueryResp> remaining = client_v2.queryAsync(request);

            Assertions.assertTrue(cancelled.cancel(true));
            Assertions.assertFalse(schemaFuture.isCancelled());

            schemaFuture.set(describeCollectionResponse());
            Assertions.assertNotNull(remaining.get(1, TimeUnit.SECONDS));
            verify(futureStub).describeCollection(any(DescribeCollectionRequest.class));
            verify(futureStub).query(any(QueryRequest.class));
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncKeepsCapturedRequestIdWhenSynchronousSchemaLoadCompletes() throws Exception {
        SchemaCache.getInstance().clear();
        ThreadLocal<String> requestId = configureClientRequestId();
        CountDownLatch schemaLoadStarted = new CountDownLatch(1);
        CountDownLatch releaseSchemaLoad = new CountDownLatch(1);
        when(blockingStub.describeCollection(any())).thenAnswer(invocation -> {
            schemaLoadStarted.countDown();
            if (!releaseSchemaLoad.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to finish the synchronous schema load");
            }
            return describeCollectionResponse();
        });
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .ids(Collections.singletonList(1L))
                .build();

        CompletableFuture<QueryResp> synchronous = CompletableFuture.supplyAsync(() -> {
            requestId.set("11111111111111111111111111111111");
            return client_v2.query(request);
        });

        try {
            Assertions.assertTrue(schemaLoadStarted.await(5, TimeUnit.SECONDS));
            requestId.set("async-query-request");
            CompletableFuture<QueryResp> asynchronous = client_v2.queryAsync(request);

            requestId.set("33333333333333333333333333333333");
            releaseSchemaLoad.countDown();

            Assertions.assertNotNull(synchronous.get(1, TimeUnit.SECONDS));
            Assertions.assertNotNull(asynchronous.get(1, TimeUnit.SECONDS));
            verify(futureStub).withOption(
                    ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                    "async-query-request");
            verify(futureStub, never()).withOption(
                    ClientRequestInterceptor.CLIENT_REQUEST_ID_OPTION,
                    "11111111111111111111111111111111");
        } finally {
            releaseSchemaLoad.countDown();
            requestId.remove();
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncUsesPreparedRequestAfterSchemaLoad() throws Exception {
        SchemaCache.getInstance().clear();
        SettableFuture<DescribeCollectionResponse> schemaFuture = SettableFuture.create();
        when(futureStub.describeCollection(any())).thenReturn(schemaFuture);
        List<String> outputFields = new ArrayList<>(Collections.singletonList("original_field"));
        QueryReq request = QueryReq.builder()
                .databaseName("original_db")
                .collectionName("original_collection")
                .ids(Collections.singletonList(1L))
                .outputFields(outputFields)
                .limit(10)
                .build();

        try {
            CompletableFuture<QueryResp> first = client_v2.session("cluster-a").queryAsync(request);
            CompletableFuture<QueryResp> second = client_v2.session("cluster-b").queryAsync(request);
            Assertions.assertNull(request.getClusterId());

            request.setDatabaseName("mutated_db");
            request.setCollectionName("mutated_collection");
            request.setClusterId("cluster-c");
            request.setLimit(20);
            outputFields.add("mutated_field");

            schemaFuture.set(describeCollectionResponse());
            first.get(1, TimeUnit.SECONDS);
            second.get(1, TimeUnit.SECONDS);

            ArgumentCaptor<QueryRequest> queryCaptor = ArgumentCaptor.forClass(QueryRequest.class);
            verify(futureStub, times(2)).query(queryCaptor.capture());
            Set<String> clusterIds = new HashSet<>();
            for (QueryRequest rpcRequest : queryCaptor.getAllValues()) {
                Assertions.assertEquals("original_db", rpcRequest.getDbName());
                Assertions.assertEquals("original_collection", rpcRequest.getCollectionName());
                Assertions.assertEquals(Collections.singletonList("original_field"),
                        rpcRequest.getOutputFieldsList());
                Assertions.assertEquals("10", getParam(rpcRequest.getQueryParamsList(), Constant.LIMIT));
                clusterIds.add(getParam(rpcRequest.getQueryParamsList(), Constant.CLUSTER_ID));
            }
            Assertions.assertEquals(new HashSet<>(Arrays.asList("cluster-a", "cluster-b")), clusterIds);
        } finally {
            SchemaCache.getInstance().clear();
        }
    }

    @Test
    void testQueryAsyncCancellationPropagatesToQueryRpc() {
        SettableFuture<QueryResults> grpcFuture = SettableFuture.create();
        when(futureStub.query(any())).thenReturn(grpcFuture);
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .build();

        CompletableFuture<QueryResp> resultFuture = client_v2.queryAsync(request);
        Assertions.assertTrue(resultFuture.cancel(true));

        Assertions.assertTrue(grpcFuture.isCancelled());
    }

    @Test
    void testQueryAsyncValidationFailureCompletesExceptionally() {
        QueryReq request = QueryReq.builder()
                .collectionName("book")
                .filter("id > 0")
                .ids(Collections.singletonList(1L))
                .build();

        CompletableFuture<QueryResp> future = Assertions.assertDoesNotThrow(
                () -> client_v2.queryAsync(request));
        ExecutionException exception = Assertions.assertThrows(ExecutionException.class,
                () -> future.get(1, TimeUnit.SECONDS));

        Assertions.assertTrue(exception.getCause() instanceof MilvusClientException);
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS,
                ((MilvusClientException) exception.getCause()).getErrorCode());
        verify(futureStub, never()).query(any(QueryRequest.class));
    }

    private DescribeCollectionResponse describeCollectionResponse() {
        return DescribeCollectionResponse.newBuilder()
                .setStatus(Status.newBuilder().setCode(0).build())
                .setCollectionName("book")
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Int64)
                                .setIsPrimaryKey(true)
                                .build())
                        .addFields(FieldSchema.newBuilder()
                                .setName("vector")
                                .setDataType(DataType.FloatVector)
                                .addTypeParams(KeyValuePair.newBuilder()
                                        .setKey("dim")
                                        .setValue("2")
                                        .build())
                                .build())
                        .build())
                .build();
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

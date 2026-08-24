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

package io.milvus.telemetry;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.ConnectRequest;
import io.milvus.grpc.ConnectResponse;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionRequest;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchRequest;
import io.milvus.grpc.SearchResultData;
import io.milvus.grpc.SearchResults;
import io.milvus.grpc.ScalarField;
import io.milvus.param.ConnectParam;
import io.milvus.param.Constant;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RetryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.highlevel.dml.GetIdsParam;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogicalOperationTelemetryTest {
    @Test
    void syncAndAsyncRetriesEachProduceOneLogicalMetric() throws Exception {
        AtomicInteger syncAttempts = new AtomicInteger();
        AtomicInteger asyncAttempts = new AtomicInteger();
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new MilvusServiceGrpc.MilvusServiceImplBase() {
                    @Override
                    public void search(
                            SearchRequest request, StreamObserver<SearchResults> responseObserver) {
                        AtomicInteger attempts = "sync".equals(request.getCollectionName())
                                ? syncAttempts : asyncAttempts;
                        if (attempts.incrementAndGet() == 1) {
                            responseObserver.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
                            return;
                        }
                        responseObserver.onNext(successfulSearch());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        ClientTelemetryManager manager = new ClientTelemetryManager(
                TelemetryConfig.defaults(), "", "test", () -> "default", null);
        MilvusClientV2 client = new MilvusClientV2(null);
        try {
            setField(client, "telemetry", manager);
            Channel intercepted = ClientInterceptors.intercept(
                    channel, new TelemetryInterceptor(manager, null));
            client.setBlockingStub(MilvusServiceGrpc.newBlockingStub(intercepted));
            client.setFutureStub(MilvusServiceGrpc.newFutureStub(intercepted));
            client.retryConfig(RetryConfig.builder()
                    .maxRetryTimes(2)
                    .initialBackOffMs(0)
                    .maxBackOffMs(0)
                    .build());

            client.search(searchRequest("sync"));
            invokeCreateSnapshot(manager);
            assertEquals(2, syncAttempts.get());
            assertEquals(1, searchRequestCount(manager));

            client.searchAsync(searchRequest("async")).get(5, TimeUnit.SECONDS);
            awaitRequestCount(manager, "Search", 2);
            assertEquals(2, asyncAttempts.get());
            assertEquals(2, searchRequestCount(manager));
        } finally {
            client.close();
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    @Test
    void legacyRetryValidationAsyncCompletionAndSimpleGetAreLogicalOnce() throws Exception {
        AtomicInteger retryAttempts = new AtomicInteger();
        AtomicInteger queryAttempts = new AtomicInteger();
        Server server = ServerBuilder.forPort(0)
                .addService(new MilvusServiceGrpc.MilvusServiceImplBase() {
                    @Override
                    public void connect(ConnectRequest request, StreamObserver<ConnectResponse> observer) {
                        observer.onNext(ConnectResponse.newBuilder()
                                .setStatus(io.milvus.grpc.Status.newBuilder().setCode(0).build())
                                .setIdentifier(1L)
                                .build());
                        observer.onCompleted();
                    }

                    @Override
                    public void search(SearchRequest request, StreamObserver<SearchResults> observer) {
                        if ("retry".equals(request.getCollectionName())
                                && retryAttempts.incrementAndGet() == 1) {
                            observer.onError(io.grpc.Status.UNAVAILABLE.asRuntimeException());
                            return;
                        }
                        if ("async".equals(request.getCollectionName())) {
                            try {
                                Thread.sleep(25L);
                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        observer.onNext(successfulSearch());
                        observer.onCompleted();
                    }

                    @Override
                    public void describeCollection(
                            DescribeCollectionRequest request,
                            StreamObserver<DescribeCollectionResponse> observer) {
                        observer.onNext(DescribeCollectionResponse.newBuilder()
                                .setStatus(io.milvus.grpc.Status.newBuilder().setCode(0).build())
                                .setCollectionName(request.getCollectionName())
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
                                                        .setKey(Constant.VECTOR_DIM)
                                                        .setValue("2")
                                                        .build())
                                                .build())
                                        .build())
                                .build());
                        observer.onCompleted();
                    }

                    @Override
                    public void query(QueryRequest request, StreamObserver<QueryResults> observer) {
                        queryAttempts.incrementAndGet();
                        observer.onNext(QueryResults.newBuilder()
                                .setStatus(io.milvus.grpc.Status.newBuilder().setCode(0).build())
                                .addFieldsData(FieldData.newBuilder()
                                        .setFieldName("id")
                                        .setType(DataType.Int64)
                                        .setScalars(ScalarField.newBuilder()
                                                .setLongData(LongArray.newBuilder().addData(1L).build())
                                                .build())
                                        .build())
                                .build());
                        observer.onCompleted();
                    }
                })
                .build()
                .start();
        MilvusServiceClient owner = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(server.getPort())
                .build());
        MilvusServiceClient client = (MilvusServiceClient) owner.withRetry(RetryParam.newBuilder()
                .withMaxRetryTimes(2)
                .withInitialBackOffMs(1)
                .withMaxBackOffMs(1)
                .build());
        ClientTelemetryManager manager = owner.getTelemetry();
        try {
            R<SearchResults> retryResult = client.search(legacySearch("retry"));
            assertEquals(R.Status.Success.getCode(), retryResult.getStatus());
            invokeCreateSnapshot(manager);
            assertEquals(2, retryAttempts.get());
            assertEquals(1, requestCount(manager, "Search"));

            R<SearchResults> invalidResult = client.search((SearchParam) null);
            assertTrue(invalidResult.getStatus() != R.Status.Success.getCode());
            invokeCreateSnapshot(manager);
            assertEquals(2, requestCount(manager, "Search"));
            assertEquals(1, errorCount(manager, "Search"));

            R<SearchResults> asyncResult = client.searchAsync(legacySearch("async"))
                    .get(5, TimeUnit.SECONDS);
            assertEquals(R.Status.Success.getCode(), asyncResult.getStatus());
            awaitRequestCount(manager, "Search", 3);
            assertEquals(3, requestCount(manager, "Search"));
            assertTrue(maxLatencyMs(manager, "Search") >= 10.0);

            R<?> getResult = client.get(GetIdsParam.newBuilder()
                    .withCollectionName("books")
                    .withPrimaryIds(Collections.singletonList(1L))
                    .withOutputFields(Collections.singletonList("id"))
                    .build());
            assertEquals(R.Status.Success.getCode(), getResult.getStatus(),
                    String.valueOf(getResult.getException()));
            invokeCreateSnapshot(manager);
            assertEquals(1, queryAttempts.get());
            assertEquals(1, requestCount(manager, "Query"));
        } finally {
            owner.close(1);
            server.shutdownNow();
        }
    }

    private static SearchReq searchRequest(String collection) {
        return SearchReq.builder()
                .collectionName(collection)
                .data(Collections.singletonList(new FloatVec(Arrays.asList(1.0f, 2.0f))))
                .limit(1)
                .build();
    }

    private static SearchParam legacySearch(String collection) {
        return SearchParam.newBuilder()
                .withCollectionName(collection)
                .withMetricType(MetricType.L2)
                .withTopK(1)
                .withVectors(Collections.singletonList(Arrays.asList(1.0f, 2.0f)))
                .withVectorFieldName("vector")
                .build();
    }

    private static SearchResults successfulSearch() {
        return SearchResults.newBuilder()
                .setStatus(io.milvus.grpc.Status.newBuilder().setCode(0).build())
                .setResults(SearchResultData.newBuilder()
                        .setNumQueries(1)
                        .addTopks(0)
                        .build())
                .build();
    }

    private static long searchRequestCount(ClientTelemetryManager manager) {
        return requestCount(manager, "Search");
    }

    private static void awaitRequestCount(
            ClientTelemetryManager manager, String operationName, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + 1_000;
        while (requestCount(manager, operationName) < expected
                && System.currentTimeMillis() < deadline) {
            invokeCreateSnapshot(manager);
            // A future may complete just before its telemetry callback. Avoid a tight loop:
            // more than 120 empty snapshots would evict the already-verified prior window.
            Thread.sleep(10L);
        }
    }

    private static long requestCount(ClientTelemetryManager manager, String operationName) {
        long count = 0;
        for (ClientTelemetryManager.MetricsSnapshot snapshot : manager.getMetricsSnapshots()) {
            for (ClientTelemetryManager.OperationSnapshot operation : snapshot.metrics) {
                if (operationName.equals(operation.operation)) {
                    count += operation.global.request_count;
                }
            }
        }
        return count;
    }

    private static long errorCount(ClientTelemetryManager manager, String operationName) {
        long count = 0;
        for (ClientTelemetryManager.MetricsSnapshot snapshot : manager.getMetricsSnapshots()) {
            for (ClientTelemetryManager.OperationSnapshot operation : snapshot.metrics) {
                if (operationName.equals(operation.operation)) {
                    count += operation.global.error_count;
                }
            }
        }
        return count;
    }

    private static double maxLatencyMs(ClientTelemetryManager manager, String operationName) {
        double max = 0.0;
        for (ClientTelemetryManager.MetricsSnapshot snapshot : manager.getMetricsSnapshots()) {
            for (ClientTelemetryManager.OperationSnapshot operation : snapshot.metrics) {
                if (operationName.equals(operation.operation)) {
                    max = Math.max(max, operation.global.max_latency_ms);
                }
            }
        }
        return max;
    }

    private static void invokeCreateSnapshot(ClientTelemetryManager manager) throws Exception {
        Method method = ClientTelemetryManager.class.getDeclaredMethod("createSnapshot");
        method.setAccessible(true);
        method.invoke(manager);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}

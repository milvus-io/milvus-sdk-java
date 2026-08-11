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

package io.milvus.v2.service.vector;

import com.google.gson.JsonObject;
import io.grpc.StatusRuntimeException;
import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionRequest;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.InsertRequest;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.Status;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VectorServiceCacheTest {
    @BeforeEach
    @AfterEach
    void clearCaches() {
        SchemaCache.getInstance().clear();
        CollectionTsCache.getInstance().clear();
    }

    @Test
    void servicesForSameEndpointShareSchemaAndTimestampCaches() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status success = Status.newBuilder().setCode(0).build();
        DescribeCollectionResponse description = DescribeCollectionResponse.newBuilder()
                .setStatus(success)
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Int64)
                                .setIsPrimaryKey(true))
                        .build())
                .build();
        when(stub.describeCollection(any())).thenReturn(description);
        when(stub.insert(any())).thenReturn(MutationResult.newBuilder()
                .setStatus(success)
                .setInsertCnt(1L)
                .setTimestamp(100L)
                .build());

        VectorService first = service("host:19530");
        VectorService second = service("host:19530");
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        InsertReq request = InsertReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(row))
                .build();

        first.insert(stub, request);
        second.insert(stub, request);

        verify(stub, times(1)).describeCollection(any());
        assertEquals(100L, CollectionTsCache.getInstance().get("host:19530", "db", "coll"));
        QueryRequest query = second.vectorUtils.ConvertToGrpcQueryRequest(QueryReq.builder()
                .collectionName("coll")
                .consistencyLevel(ConsistencyLevel.SESSION)
                .build());
        assertEquals(100L, query.getGuaranteeTimestamp());
    }

    @Test
    void emptyDatabaseIsOmittedFromRpcAndNormalizedInCacheKey() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status success = Status.newBuilder().setCode(0).build();
        DescribeCollectionResponse description = description(1L);
        when(stub.describeCollection(any())).thenReturn(description);
        when(stub.insert(any())).thenReturn(MutationResult.newBuilder()
                .setStatus(success)
                .setInsertCnt(1L)
                .setTimestamp(100L)
                .build());

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        VectorService service = service("serverless:443", null);
        service.insert(stub, InsertReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(row))
                .build());

        ArgumentCaptor<DescribeCollectionRequest> describeCaptor =
                ArgumentCaptor.forClass(DescribeCollectionRequest.class);
        verify(stub).describeCollection(describeCaptor.capture());
        assertEquals("", describeCaptor.getValue().getDbName());

        ArgumentCaptor<InsertRequest> insertCaptor = ArgumentCaptor.forClass(InsertRequest.class);
        verify(stub).insert(insertCaptor.capture());
        assertEquals("", insertCaptor.getValue().getDbName());

        assertSame(description, SchemaCache.getInstance().get("serverless:443", "", "coll"));
        assertSame(description, SchemaCache.getInstance().get("serverless:443", "default", "coll"));
        assertEquals(100L, CollectionTsCache.getInstance().get(
                "serverless:443", "default", "coll"));
    }

    @Test
    void clientsWithDifferentDeadlinesDoNotShareInflightSchemaLoads() throws Exception {
        MilvusServiceGrpc.MilvusServiceBlockingStub noDeadlineStub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        MilvusServiceGrpc.MilvusServiceBlockingStub deadlineStub =
                mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        Status success = Status.newBuilder().setCode(0).build();

        when(noDeadlineStub.describeCollection(any())).thenAnswer(invocation -> {
            loaderStarted.countDown();
            assertTrue(releaseLoader.await(5, TimeUnit.SECONDS));
            return description(1L);
        });
        when(noDeadlineStub.insert(any())).thenReturn(MutationResult.newBuilder()
                .setStatus(success)
                .setInsertCnt(1L)
                .build());
        when(deadlineStub.describeCollection(any()))
                .thenThrow(io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException());

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        InsertReq request = InsertReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(row))
                .build();
        VectorService noDeadlineClient = service("host:19530");
        VectorService deadlineClient = service("host:19530");
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<InsertResp> noDeadlineFuture = executor.submit(
                    () -> noDeadlineClient.insert(noDeadlineStub, request));
            assertTrue(loaderStarted.await(5, TimeUnit.SECONDS));

            Future<InsertResp> deadlineFuture = executor.submit(() -> deadlineClient.insert(deadlineStub, request));
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> deadlineFuture.get(5, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof StatusRuntimeException);
            StatusRuntimeException statusException = (StatusRuntimeException) failure.getCause();
            assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED, statusException.getStatus().getCode());

            releaseLoader.countDown();
            assertEquals(1L, noDeadlineFuture.get(5, TimeUnit.SECONDS).getInsertCnt());
            verify(noDeadlineStub, times(1)).describeCollection(any());
            verify(deadlineStub, times(1)).describeCollection(any());
        } finally {
            releaseLoader.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void schemaMismatchRetriesOnlyOnceAndLeavesFailedSchemaUncached() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status mismatch = Status.newBuilder()
                .setErrorCode(io.milvus.grpc.ErrorCode.SchemaMismatch)
                .setReason("schema mismatch")
                .build();
        DescribeCollectionResponse cached = description(1L);
        DescribeCollectionResponse refreshed = description(2L);
        SchemaCache.getInstance().set("host:19530", "db", "coll", cached);
        when(stub.describeCollection(any())).thenReturn(refreshed);
        when(stub.insert(any())).thenReturn(MutationResult.newBuilder().setStatus(mismatch).build());

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        InsertReq request = InsertReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(row))
                .build();

        assertThrows(MilvusClientException.class, () -> service("host:19530").insert(stub, request));
        verify(stub, times(2)).insert(any());
        verify(stub, times(1)).describeCollection(any());
        assertNull(SchemaCache.getInstance().get("host:19530", "db", "coll"));
    }

    @Test
    void invalidInsertAndUpsertResponsesDoNotInvalidateSchema() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status failure = Status.newBuilder().setCode(1).setReason("invalid data").build();
        DescribeCollectionResponse cached = description(1L);
        SchemaCache.getInstance().set("host:19530", "db", "coll", cached);
        when(stub.insert(any())).thenReturn(MutationResult.newBuilder().setStatus(failure).build());
        when(stub.upsert(any())).thenReturn(MutationResult.newBuilder().setStatus(failure).build());

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        InsertReq insert = InsertReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(row))
                .build();
        UpsertReq upsert = UpsertReq.builder()
                .collectionName("coll")
                .data(Collections.singletonList(row))
                .build();

        VectorService service = service("host:19530");
        assertThrows(MilvusClientException.class, () -> service.insert(stub, insert));
        assertSame(cached, SchemaCache.getInstance().get("host:19530", "db", "coll"));

        assertThrows(MilvusClientException.class, () -> service.upsert(stub, upsert));
        assertSame(cached, SchemaCache.getInstance().get("host:19530", "db", "coll"));
    }

    @Test
    void failedFilterDeleteDoesNotInvalidateSchema() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status failure = Status.newBuilder().setCode(1).setReason("delete failed").build();
        DescribeCollectionResponse cached = description(1L);
        SchemaCache.getInstance().set("host:19530", "db", "coll", cached);
        when(stub.delete(any())).thenReturn(MutationResult.newBuilder().setStatus(failure).build());

        DeleteReq request = DeleteReq.builder()
                .collectionName("coll")
                .filter("id > 0")
                .build();

        assertThrows(MilvusClientException.class, () -> service("host:19530").delete(stub, request));
        assertSame(cached, SchemaCache.getInstance().get("host:19530", "db", "coll"));
    }

    @Test
    void failedDeleteByIdsDoesNotInvalidateSchema() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status failure = Status.newBuilder().setCode(1).setReason("delete failed").build();
        DescribeCollectionResponse cached = description(1L);
        SchemaCache.getInstance().set("host:19530", "db", "coll", cached);
        when(stub.delete(any())).thenReturn(MutationResult.newBuilder().setStatus(failure).build());

        DeleteReq request = DeleteReq.builder()
                .collectionName("coll")
                .ids(Collections.singletonList(1L))
                .build();

        assertThrows(MilvusClientException.class, () -> service("host:19530").delete(stub, request));
        assertSame(cached, SchemaCache.getInstance().get("host:19530", "db", "coll"));
    }

    private static DescribeCollectionResponse description(long collectionId) {
        Status success = Status.newBuilder().setCode(0).build();
        return DescribeCollectionResponse.newBuilder()
                .setStatus(success)
                .setCollectionID(collectionId)
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Int64)
                                .setIsPrimaryKey(true))
                        .build())
                .build();
    }

    private static VectorService service(String endpoint) {
        return service(endpoint, "db");
    }

    private static VectorService service(String endpoint, String databaseName) {
        VectorService service = new VectorService();
        service.setEndpoint(endpoint);
        service.setCurrentDbName(databaseName);
        return service;
    }
}

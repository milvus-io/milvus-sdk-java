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

package io.milvus.client;

import com.google.gson.JsonObject;
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
import io.milvus.grpc.Status;
import io.milvus.param.LogLevel;
import io.milvus.param.R;
import io.milvus.param.RetryParam;
import io.milvus.param.RpcStatus;
import io.milvus.param.alias.AlterAliasParam;
import io.milvus.param.alias.CreateAliasParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.RenameCollectionParam;
import io.milvus.param.dml.InsertParam;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractMilvusGrpcClientCacheTest {
    @BeforeEach
    @AfterEach
    void clearCache() {
        CollectionTsCache.getInstance().clear();
        SchemaCache.getInstance().clear();
    }

    @Test
    void crossDatabaseRenameMovesTimestamp() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.renameCollection(any())).thenReturn(Status.newBuilder().setCode(0).build());
        TestClient client = new TestClient(stub);
        CollectionTsCache.getInstance().set(client.cacheEndpoint(), "source", "old", 100L);

        R<RpcStatus> response = client.renameCollection(RenameCollectionParam.newBuilder()
                .withOldDatabaseName("source")
                .withOldCollectionName("old")
                .withNewDatabaseName("target")
                .withNewCollectionName("new")
                .build());

        assertEquals(R.Status.Success.getCode(), response.getStatus());
        assertEquals(0L, CollectionTsCache.getInstance().get(client.cacheEndpoint(), "source", "old"));
        assertEquals(100L, CollectionTsCache.getInstance().get(client.cacheEndpoint(), "target", "new"));
    }

    @Test
    void createAndAlterAliasCopyCollectionTimestamp() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status success = Status.newBuilder().setCode(0).build();
        when(stub.createAlias(any())).thenReturn(success);
        when(stub.alterAlias(any())).thenReturn(success);
        TestClient client = new TestClient(stub);

        CollectionTsCache.getInstance().set(client.cacheEndpoint(), "db", "first", 100L);
        assertEquals(R.Status.Success.getCode(), client.createAlias(CreateAliasParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("first")
                .withAlias("alias")
                .build()).getStatus());
        assertEquals(100L, CollectionTsCache.getInstance().get(client.cacheEndpoint(), "db", "alias"));

        CollectionTsCache.getInstance().set(client.cacheEndpoint(), "db", "second", 200L);
        assertEquals(R.Status.Success.getCode(), client.alterAlias(AlterAliasParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("second")
                .withAlias("alias")
                .build()).getStatus());
        assertEquals(200L, CollectionTsCache.getInstance().get(client.cacheEndpoint(), "db", "alias"));
    }

    @Test
    void successfulCreateInvalidatesStaleSchemaAndPreservesTimestamp() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        when(stub.createCollection(any())).thenReturn(Status.newBuilder().setCode(0).build());
        TestClient client = new TestClient(stub);
        SchemaCache.getInstance().set(client.cacheEndpoint(), "db", "coll",
                DescribeCollectionResponse.newBuilder().setCollectionID(1L).build());
        CollectionTsCache.getInstance().set(client.cacheEndpoint(), "db", "coll", 100L);

        R<RpcStatus> response = client.createCollection(CreateCollectionParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .addFieldType(FieldType.newBuilder()
                        .withName("id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .build())
                .build());

        assertEquals(R.Status.Success.getCode(), response.getStatus());
        assertNull(SchemaCache.getInstance().get(client.cacheEndpoint(), "db", "coll"));
        assertEquals(100L, CollectionTsCache.getInstance().get(client.cacheEndpoint(), "db", "coll"));
    }

    @Test
    void invalidInsertResponseDoesNotInvalidateSchema() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status failure = Status.newBuilder().setCode(1).setReason("invalid data").build();
        TestClient client = new TestClient(stub);
        DescribeCollectionResponse cached = description(Status.newBuilder().setCode(0).build(), 100L);
        SchemaCache.getInstance().set(client.cacheEndpoint(), "default", "coll", cached);
        when(stub.insert(any())).thenReturn(MutationResult.newBuilder().setStatus(failure).build());

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        R<MutationResult> response = client.insert(InsertParam.newBuilder()
                .withCollectionName("coll")
                .withRows(Collections.singletonList(row))
                .build());

        assertNotEquals(R.Status.Success.getCode(), response.getStatus());
        assertSame(cached, SchemaCache.getInstance().get(client.cacheEndpoint(), "default", "coll"));
    }

    @Test
    void subclassesUseTheirProvidedCacheEndpoints() {
        MilvusServiceGrpc.MilvusServiceBlockingStub firstStub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        MilvusServiceGrpc.MilvusServiceBlockingStub secondStub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status success = Status.newBuilder().setCode(0).build();
        when(firstStub.describeCollection(any())).thenReturn(description(success, 100L));
        when(secondStub.describeCollection(any())).thenReturn(description(success, 200L));
        when(firstStub.insert(any())).thenReturn(MutationResult.newBuilder().setStatus(success).build());
        when(secondStub.insert(any())).thenReturn(MutationResult.newBuilder().setStatus(success).build());
        TestClient firstClient = new TestClient(firstStub, "first:19530");
        TestClient secondClient = new TestClient(secondStub, "second:19530");

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        InsertParam request = InsertParam.newBuilder()
                .withCollectionName("coll")
                .withRows(Collections.singletonList(row))
                .build();

        assertEquals(R.Status.Success.getCode(), firstClient.insert(request).getStatus());
        assertEquals(R.Status.Success.getCode(), secondClient.insert(request).getStatus());

        assertNotEquals(firstClient.cacheEndpoint(), secondClient.cacheEndpoint());
        verify(firstStub, times(1)).describeCollection(any());
        verify(secondStub, times(1)).describeCollection(any());
        ArgumentCaptor<InsertRequest> secondRequest = ArgumentCaptor.forClass(InsertRequest.class);
        verify(secondStub).insert(secondRequest.capture());
        assertEquals(200L, secondRequest.getValue().getSchemaTimestamp());
    }

    @Test
    void emptyDatabaseIsOmittedFromRpcAndNormalizedInCacheKey() {
        MilvusServiceGrpc.MilvusServiceBlockingStub stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status success = Status.newBuilder().setCode(0).build();
        DescribeCollectionResponse description = description(success, 100L);
        when(stub.describeCollection(any())).thenReturn(description);
        when(stub.insert(any())).thenReturn(MutationResult.newBuilder()
                .setStatus(success)
                .setInsertCnt(1L)
                .setTimestamp(100L)
                .build());
        TestClient client = new TestClient(stub, "serverless:443", null);

        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        R<MutationResult> response = client.insert(InsertParam.newBuilder()
                .withCollectionName("coll")
                .withRows(Collections.singletonList(row))
                .build());

        assertEquals(R.Status.Success.getCode(), response.getStatus());
        ArgumentCaptor<DescribeCollectionRequest> describeCaptor =
                ArgumentCaptor.forClass(DescribeCollectionRequest.class);
        verify(stub).describeCollection(describeCaptor.capture());
        assertEquals("", describeCaptor.getValue().getDbName());

        ArgumentCaptor<InsertRequest> insertCaptor = ArgumentCaptor.forClass(InsertRequest.class);
        verify(stub).insert(insertCaptor.capture());
        assertEquals("", insertCaptor.getValue().getDbName());

        assertSame(description, SchemaCache.getInstance().get(client.cacheEndpoint(), "", "coll"));
        assertSame(description, SchemaCache.getInstance().get(client.cacheEndpoint(), "default", "coll"));
        assertEquals(100L, CollectionTsCache.getInstance().get(
                client.cacheEndpoint(), "default", "coll"));
    }

    private static DescribeCollectionResponse description(Status status, long updateTimestamp) {
        return DescribeCollectionResponse.newBuilder()
                .setStatus(status)
                .setUpdateTimestamp(updateTimestamp)
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("id")
                                .setDataType(DataType.Int64)
                                .setIsPrimaryKey(true)))
                .build();
    }

    private static class TestClient extends AbstractMilvusGrpcClient {
        private final MilvusServiceGrpc.MilvusServiceBlockingStub stub;
        private final String endpoint;
        private final String databaseName;

        private TestClient(MilvusServiceGrpc.MilvusServiceBlockingStub stub) {
            this(stub, "test:19530", "default");
        }

        private TestClient(MilvusServiceGrpc.MilvusServiceBlockingStub stub, String endpoint) {
            this(stub, endpoint, "default");
        }

        private TestClient(MilvusServiceGrpc.MilvusServiceBlockingStub stub,
                           String endpoint, String databaseName) {
            this.stub = stub;
            this.endpoint = endpoint;
            this.databaseName = databaseName;
        }

        @Override
        protected MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub() {
            return stub;
        }

        @Override
        protected MilvusServiceGrpc.MilvusServiceFutureStub futureStub() {
            return null;
        }

        @Override
        protected boolean clientIsReady() {
            return true;
        }

        @Override
        protected String currentDbName() {
            return databaseName;
        }

        @Override
        protected String currentEndpoint() {
            return endpoint;
        }

        private String cacheEndpoint() {
            return currentEndpoint();
        }

        @Override
        public MilvusClient withTimeout(long timeout, TimeUnit timeoutUnit) {
            return this;
        }

        @Override
        public MilvusClient withRetry(RetryParam retryParam) {
            return this;
        }

        @Override
        public MilvusClient withRetry(int retryTimes) {
            return this;
        }

        @Override
        public MilvusClient withRetryInterval(long interval, TimeUnit timeUnit) {
            return this;
        }

        @Override
        public void setLogLevel(LogLevel level) {
        }

        @Override
        public void close(long maxWaitSeconds) {
        }
    }
}

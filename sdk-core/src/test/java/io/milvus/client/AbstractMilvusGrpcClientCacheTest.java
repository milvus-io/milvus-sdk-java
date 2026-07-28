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
    void legacySubclassesUseInstanceScopedCacheEndpoints() {
        MilvusServiceGrpc.MilvusServiceBlockingStub firstStub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        MilvusServiceGrpc.MilvusServiceBlockingStub secondStub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        Status success = Status.newBuilder().setCode(0).build();
        when(firstStub.describeCollection(any())).thenReturn(description(success, 100L));
        when(secondStub.describeCollection(any())).thenReturn(description(success, 200L));
        when(firstStub.insert(any())).thenReturn(MutationResult.newBuilder().setStatus(success).build());
        when(secondStub.insert(any())).thenReturn(MutationResult.newBuilder().setStatus(success).build());
        TestClient firstClient = new TestClient(firstStub);
        TestClient secondClient = new TestClient(secondStub);

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

        private TestClient(MilvusServiceGrpc.MilvusServiceBlockingStub stub) {
            this.stub = stub;
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
            return "default";
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

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
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DeleteRequest;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.Status;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.QueryReq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class VectorIdExpressionTest extends BaseTest {
    private static final String COLLECTION = "escaped_ids";
    private static final List<Object> IDS = Arrays.asList("a\"b", "a\\b", "line\nbreak", "line\rbreak");
    private static final String EXPRESSION = "pk in [\"a\\\"b\",\"a\\\\b\",\"line\\nbreak\",\"line\\rbreak\"]";

    @BeforeEach
    void useVarcharPrimaryKey() {
        SchemaCache.getInstance().clear();
        DescribeCollectionResponse schema = DescribeCollectionResponse.newBuilder()
                .setStatus(Status.newBuilder().setCode(0))
                .setCollectionName(COLLECTION)
                .setSchema(CollectionSchema.newBuilder()
                        .addFields(FieldSchema.newBuilder()
                                .setName("pk")
                                .setDataType(DataType.VarChar)
                                .setIsPrimaryKey(true)))
                .build();
        when(blockingStub.describeCollection(any())).thenReturn(schema);
        when(futureStub.describeCollection(any())).thenReturn(Futures.immediateFuture(schema));
    }

    @AfterEach
    void clearSchemaCache() {
        SchemaCache.getInstance().clear();
    }

    @Test
    void queryEscapesIdsInRpcExpression() {
        client_v2.query(QueryReq.builder().collectionName(COLLECTION).ids(IDS).build());

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        assertEquals(EXPRESSION, captor.getValue().getExpr());
    }

    @Test
    void queryAsyncEscapesIdsInRpcExpression() throws Exception {
        client_v2.queryAsync(QueryReq.builder().collectionName(COLLECTION).ids(IDS).build())
                .get(1, TimeUnit.SECONDS);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(futureStub).query(captor.capture());
        assertEquals(EXPRESSION, captor.getValue().getExpr());
    }

    @Test
    void getEscapesIdsInRpcExpression() {
        client_v2.get(GetReq.builder().collectionName(COLLECTION).ids(IDS).build());

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(blockingStub).query(captor.capture());
        assertEquals(EXPRESSION, captor.getValue().getExpr());
    }

    @Test
    void getAsyncEscapesIdsInRpcExpression() throws Exception {
        client_v2.getAsync(GetReq.builder().collectionName(COLLECTION).ids(IDS).build())
                .get(1, TimeUnit.SECONDS);

        ArgumentCaptor<QueryRequest> captor = ArgumentCaptor.forClass(QueryRequest.class);
        verify(futureStub).query(captor.capture());
        assertEquals(EXPRESSION, captor.getValue().getExpr());
    }

    @Test
    void deleteEscapesIdsInRpcExpression() {
        client_v2.delete(DeleteReq.builder().collectionName(COLLECTION).ids(IDS).build());

        ArgumentCaptor<DeleteRequest> captor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(blockingStub).delete(captor.capture());
        assertEquals(EXPRESSION, captor.getValue().getExpr());
    }
}

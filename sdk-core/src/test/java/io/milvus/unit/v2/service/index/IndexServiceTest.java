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

package io.milvus.unit.v2.service.index;

import io.milvus.grpc.AlterIndexRequest;
import io.milvus.grpc.CreateIndexRequest;
import io.milvus.grpc.DescribeIndexRequest;
import io.milvus.grpc.DescribeIndexResponse;
import io.milvus.grpc.DropIndexRequest;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.IndexDescription;
import io.milvus.grpc.IndexState;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.Status;
import io.milvus.param.Constant;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.index.IndexService;
import io.milvus.v2.service.index.request.AlterIndexPropertiesReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexPropertiesReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("unit")
class IndexServiceTest {
    private MilvusServiceGrpc.MilvusServiceBlockingStub stub;
    private IndexService service;

    @BeforeEach
    void setUp() {
        stub = mock(MilvusServiceGrpc.MilvusServiceBlockingStub.class);
        service = new IndexService();
        service.setEndpoint("host:19530");
        service.setCurrentDbName("db");
    }

    private static Status success() {
        return Status.newBuilder().setCode(0).setErrorCode(ErrorCode.Success).build();
    }

    @Test
    void createIndexReturnsNull() {
        when(stub.createIndex(any())).thenReturn(success());
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 128);
        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .indexName("idx")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build();
        CreateIndexReq request = CreateIndexReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .indexParams(Collections.singletonList(indexParam))
                .sync(false)
                .build();

        assertNull(service.createIndex(stub, request));

        ArgumentCaptor<CreateIndexRequest> captor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(stub).createIndex(captor.capture());
        assertEquals("vector", captor.getValue().getFieldName());
        assertEquals("idx", captor.getValue().getIndexName());
        assertEquals("db", captor.getValue().getDbName());
        assertTrue(captor.getValue().getExtraParamsList().stream()
                .anyMatch(kv -> kv.getKey().equals(Constant.METRIC_TYPE)
                        && kv.getValue().equals("COSINE")));
        assertTrue(captor.getValue().getExtraParamsList().stream()
                .anyMatch(kv -> kv.getKey().equals("nlist") && kv.getValue().equals("128")));
    }

    @Test
    void createIndexRejectsEmptyIndexParams() {
        CreateIndexReq request = CreateIndexReq.builder()
                .collectionName("coll")
                .indexParams(Collections.emptyList())
                .build();

        assertThrows(MilvusClientException.class, () -> service.createIndex(stub, request));
    }

    @Test
    void dropIndexReturnsNull() {
        when(stub.dropIndex(any())).thenReturn(success());
        DropIndexReq request = DropIndexReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .fieldName("vector")
                .indexName("idx")
                .build();

        assertNull(service.dropIndex(stub, request));

        ArgumentCaptor<DropIndexRequest> captor = ArgumentCaptor.forClass(DropIndexRequest.class);
        verify(stub).dropIndex(captor.capture());
        assertEquals("idx", captor.getValue().getIndexName());
        assertEquals("vector", captor.getValue().getFieldName());
    }

    @Test
    void alterIndexPropertiesReturnsNull() {
        when(stub.alterIndex(any())).thenReturn(success());
        AlterIndexPropertiesReq request = AlterIndexPropertiesReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .indexName("idx")
                .property("mmap.enabled", "true")
                .build();

        assertNull(service.alterIndexProperties(stub, request));

        ArgumentCaptor<AlterIndexRequest> captor = ArgumentCaptor.forClass(AlterIndexRequest.class);
        verify(stub).alterIndex(captor.capture());
        assertEquals("idx", captor.getValue().getIndexName());
    }

    @Test
    void dropIndexPropertiesReturnsNull() {
        when(stub.alterIndex(any())).thenReturn(success());
        DropIndexPropertiesReq request = DropIndexPropertiesReq.builder()
                .collectionName("coll")
                .indexName("idx")
                .propertyKeys(Collections.singletonList("mmap.enabled"))
                .build();

        assertNull(service.dropIndexProperties(stub, request));

        ArgumentCaptor<AlterIndexRequest> captor = ArgumentCaptor.forClass(AlterIndexRequest.class);
        verify(stub).alterIndex(captor.capture());
        assertTrue(captor.getValue().getDeleteKeysList().contains("mmap.enabled"));
    }

    @Test
    void describeIndexReturnsDescriptions() {
        DescribeIndexResponse response = DescribeIndexResponse.newBuilder()
                .setStatus(success())
                .addIndexDescriptions(IndexDescription.newBuilder()
                        .setIndexName("idx")
                        .setFieldName("vector")
                        .setIndexID(1L)
                        .setTotalRows(100)
                        .addParams(KeyValuePair.newBuilder()
                                .setKey(Constant.INDEX_TYPE)
                                .setValue("AUTOINDEX")
                                .build())
                        .setState(IndexState.Finished)
                        .build())
                .build();
        when(stub.describeIndex(any())).thenReturn(response);
        DescribeIndexReq request = DescribeIndexReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .fieldName("vector")
                .indexName("idx")
                .build();

        DescribeIndexResp result = service.describeIndex(stub, request);

        assertEquals(1, result.getIndexDescriptions().size());
        DescribeIndexResp.IndexDesc desc = result.getIndexDescriptions().get(0);
        assertEquals("idx", desc.getIndexName());
        assertEquals("vector", desc.getFieldName());
        assertEquals(IndexParam.IndexType.AUTOINDEX, desc.getIndexType());
        verify(stub).describeIndex(any(DescribeIndexRequest.class));
    }

    @Test
    void listIndexesReturnsNames() {
        DescribeIndexResponse response = DescribeIndexResponse.newBuilder()
                .setStatus(success())
                .addIndexDescriptions(IndexDescription.newBuilder()
                        .setIndexName("idx1").setFieldName("vector1").build())
                .addIndexDescriptions(IndexDescription.newBuilder()
                        .setIndexName("idx2").setFieldName("vector2").build())
                .build();
        when(stub.describeIndex(any())).thenReturn(response);
        ListIndexesReq request = ListIndexesReq.builder().collectionName("coll").build();

        assertEquals(2, service.listIndexes(stub, request).size());
        verify(stub).describeIndex(any(DescribeIndexRequest.class));
    }

    @Test
    void listIndexesFiltersByFieldName() {
        DescribeIndexResponse response = DescribeIndexResponse.newBuilder()
                .setStatus(success())
                .addIndexDescriptions(IndexDescription.newBuilder()
                        .setIndexName("idx1").setFieldName("vector1").build())
                .addIndexDescriptions(IndexDescription.newBuilder()
                        .setIndexName("idx2").setFieldName("vector2").build())
                .build();
        when(stub.describeIndex(any())).thenReturn(response);
        ListIndexesReq request = ListIndexesReq.builder()
                .collectionName("coll")
                .fieldName("vector1")
                .build();

        assertEquals(Collections.singletonList("idx1"), service.listIndexes(stub, request));
    }

    @Test
    void listIndexesReturnsEmptyForIndexNotExist() {
        DescribeIndexResponse response = DescribeIndexResponse.newBuilder()
                .setStatus(Status.newBuilder()
                        .setErrorCode(ErrorCode.IndexNotExist)
                        .setReason("no index")
                        .build())
                .build();
        when(stub.describeIndex(any())).thenReturn(response);
        ListIndexesReq request = ListIndexesReq.builder().collectionName("coll").build();

        assertTrue(service.listIndexes(stub, request).isEmpty());
    }
}

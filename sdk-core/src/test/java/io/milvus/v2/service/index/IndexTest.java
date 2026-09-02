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

package io.milvus.v2.service.index;

import io.milvus.grpc.CreateIndexRequest;
import io.milvus.v2.BaseTest;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.index.request.AlterIndexPropertiesReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.DropIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class IndexTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(IndexTest.class);

    @Test
    void testCreateIndex() {
        // vector index
        IndexParam indexParam = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .fieldName("vector")
                .build();
        // scalar index
        IndexParam scalarIndexParam = IndexParam.builder()
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .fieldName("age")
                .build();
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParam);
        indexParams.add(scalarIndexParam);
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName("test")
                .indexParams(indexParams)
                .build();
        client_v2.createIndex(createIndexReq);
    }

    @Test
    void testCreateIndexEmptyIndexParamsRejected() {
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName("test")
                .indexParams(Collections.emptyList())
                .build();
        MilvusClientException exception = assertThrows(MilvusClientException.class,
                () -> client_v2.createIndex(createIndexReq));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        verify(blockingStub, never()).createIndex(any(CreateIndexRequest.class));
    }

    @Test
    void testCreateIndexNullFieldNameRejected() {
        // the null fieldName must be rejected with a checked error instead of a NullPointerException
        MilvusClientException exception = assertThrows(MilvusClientException.class,
                () -> IndexParam.builder().fieldName(null).build());
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testCreateIndexDimMustBeInt() {
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("dim", "abc");
        IndexParam indexParam = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .fieldName("vector")
                .extraParams(extraParams)
                .build();
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName("test")
                .indexParams(Collections.singletonList(indexParam))
                .build();
        MilvusClientException exception = assertThrows(MilvusClientException.class,
                () -> client_v2.createIndex(createIndexReq));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        verify(blockingStub, never()).createIndex(any(CreateIndexRequest.class));
    }

    @Test
    void testCreateIndexDimRejectsNumericStringAndDouble() {
        // PyMilvus create_index_request requires isinstance(dim, int); numeric strings and floats are rejected
        Map<String, Object> stringParams = new HashMap<>();
        stringParams.put("dim", "128");
        IndexParam stringDim = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .fieldName("vector")
                .extraParams(stringParams)
                .build();
        MilvusClientException stringException = assertThrows(MilvusClientException.class,
                () -> client_v2.createIndex(CreateIndexReq.builder()
                        .collectionName("test")
                        .indexParams(Collections.singletonList(stringDim))
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, stringException.getErrorCode());

        Map<String, Object> doubleParams = new HashMap<>();
        doubleParams.put("dim", 128.0);
        IndexParam doubleDim = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .fieldName("vector")
                .extraParams(doubleParams)
                .build();
        MilvusClientException doubleException = assertThrows(MilvusClientException.class,
                () -> client_v2.createIndex(CreateIndexReq.builder()
                        .collectionName("test")
                        .indexParams(Collections.singletonList(doubleDim))
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, doubleException.getErrorCode());

        verify(blockingStub, never()).createIndex(any(CreateIndexRequest.class));

        // integral types are still accepted
        Map<String, Object> intParams = new HashMap<>();
        intParams.put("dim", 128);
        IndexParam intDim = IndexParam.builder()
                .metricType(IndexParam.MetricType.COSINE)
                .fieldName("vector")
                .extraParams(intParams)
                .build();
        client_v2.createIndex(CreateIndexReq.builder()
                .collectionName("test")
                .indexParams(Collections.singletonList(intDim))
                .build());
        verify(blockingStub).createIndex(any(CreateIndexRequest.class));
    }

    @Test
    void testAlterIndexPropertiesNullPropertiesRejected() {
        AlterIndexPropertiesReq request = AlterIndexPropertiesReq.builder()
                .collectionName("test")
                .indexName("vector_idx")
                .properties(null)
                .build();
        MilvusClientException exception = assertThrows(MilvusClientException.class,
                () -> client_v2.alterIndexProperties(request));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        verify(blockingStub, never()).alterIndex(any());
    }

    @Test
    void testIndexParamSettersRoundTrip() {
        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .build();
        indexParam.setFieldName("vector2");
        indexParam.setIndexName("idx");
        indexParam.setIndexType(IndexParam.IndexType.HNSW);
        indexParam.setMetricType(IndexParam.MetricType.IP);
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("M", 16);
        indexParam.setExtraParams(extraParams);
        Assertions.assertEquals("vector2", indexParam.getFieldName());
        Assertions.assertEquals("idx", indexParam.getIndexName());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, indexParam.getIndexType());
        Assertions.assertEquals(IndexParam.MetricType.IP, indexParam.getMetricType());
        Assertions.assertEquals(16, indexParam.getExtraParams().get("M"));
    }

    @Test
    void testIndexParamNullFieldNameRejected() {
        MilvusClientException exception = assertThrows(MilvusClientException.class,
                () -> IndexParam.builder().fieldName(null).build());
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        exception = assertThrows(MilvusClientException.class,
                () -> IndexParam.builder().fieldName("vector").build().setFieldName(null));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testDescribeIndex() {
        DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        DescribeIndexResp responseR = client_v2.describeIndex(describeIndexReq);
        logger.info(responseR.toString());
    }

    @Test
    void testDropIndex() {
        DropIndexReq dropIndexReq = DropIndexReq.builder()
                .collectionName("test")
                .fieldName("vector")
                .build();
        client_v2.dropIndex(dropIndexReq);
    }

    @Test
    void testListIndexes() {
        ListIndexesReq listIndexesReq = ListIndexesReq.builder()
                .collectionName("test")
                .build();
        List<String> indexNames = client_v2.listIndexes(listIndexesReq);
        logger.info(indexNames.toString());
    }
}
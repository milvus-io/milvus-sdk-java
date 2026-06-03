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

package io.milvus.v2.service.collection;

import io.milvus.grpc.AddCollectionStructFieldRequest;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.StructArrayFieldSchema;
import io.milvus.v2.BaseTest;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.GetCollectionStatsResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.utils.SchemaUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

class CollectionTest extends BaseTest {
    Logger logger = LoggerFactory.getLogger(CollectionTest.class);

    private String getParam(List<KeyValuePair> params, String key) {
        return params.stream()
                .filter(param -> key.equals(param.getKey()))
                .map(KeyValuePair::getValue)
                .findFirst()
                .orElse(null);
    }

    @Test
    void testListCollections() {
        ListCollectionsResp a = client_v2.listCollections();
    }

    @Test
    void testCreateCollection() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .build();
        client_v2.createCollection(req);
    }

    @Test
    void testEnableDynamicSchema() {
        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("test2")
                .dimension(2)
                .enableDynamicField(false)
                .build();
        Assertions.assertFalse(req.getEnableDynamicField());

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        collectionSchema
                .addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).build())
                .addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build());

        req = CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .build();
        Assertions.assertTrue(req.getEnableDynamicField());
        Assertions.assertTrue(req.getCollectionSchema().isEnableDynamicField());

        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .enableDynamicField(false)
                .collectionSchema(collectionSchema)
                .build()
        );

        assertThrows(MilvusClientException.class, () -> CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .enableDynamicField(false)
                .build()
        );
    }

    @Test
    void testCreateCollectionWithSchema() {

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema
                .addField(AddFieldReq.builder().fieldName("id").dataType(DataType.Int64).build())
                .addField(AddFieldReq.builder().fieldName("vector").dataType(DataType.FloatVector).dimension(2).build())
                .addField(AddFieldReq.builder().fieldName("description").dataType(DataType.VarChar).maxLength(64).build());

        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .metricType(IndexParam.MetricType.L2)
                .build();
        IndexParam indexParam2 = IndexParam.builder()
                .fieldName("description")
                .indexType(IndexParam.IndexType.INVERTED)
                .build();


        CreateCollectionReq request = CreateCollectionReq.builder()
                .collectionName("test")
                .collectionSchema(collectionSchema)
                .indexParams(Arrays.asList(indexParam, indexParam2))
                .indexParam(IndexParam.builder() // fluent api, add index param
                        .fieldName("id")
                        .indexType(IndexParam.IndexType.INVERTED)
                        .build()
                )
                .build();
        client_v2.createCollection(request);

        AlterCollectionReq req = AlterCollectionReq.builder()
                .collectionName("test")
                .property("prop", "val")
                .build();
        assertEquals("val", req.getProperties().get("prop"));
    }

    @Test
    void testAddCollectionStructField() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .databaseName("default")
                .collectionName("test")
                .fieldName("metadata")
                .description("struct field")
                .maxCapacity(16)
                .addStructField(AddFieldReq.builder()
                        .fieldName("score")
                        .dataType(DataType.Float)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("embedding")
                        .dataType(DataType.FloatVector)
                        .dimension(4)
                        .build())
                .typeParam("mmap.enabled", "true")
                .typeParam("warmup", "{\"policy\":\"async\"}")
                .build();

        client_v2.addCollectionStructField(request);

        ArgumentCaptor<AddCollectionStructFieldRequest> captor = ArgumentCaptor.forClass(AddCollectionStructFieldRequest.class);
        verify(blockingStub).addCollectionStructField(captor.capture());
        AddCollectionStructFieldRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());

        StructArrayFieldSchema structSchema = rpcRequest.getStructArrayFieldSchema();
        Assertions.assertEquals("metadata", structSchema.getName());
        Assertions.assertEquals("struct field", structSchema.getDescription());
        Assertions.assertTrue(structSchema.getNullable());
        Assertions.assertEquals("true", getParam(structSchema.getTypeParamsList(), "mmap.enabled"));
        Assertions.assertEquals("{\"policy\":\"async\"}", getParam(structSchema.getTypeParamsList(), "warmup"));

        FieldSchema scoreField = structSchema.getFields(0);
        Assertions.assertEquals("score", scoreField.getName());
        Assertions.assertEquals(io.milvus.grpc.DataType.Array, scoreField.getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.Float, scoreField.getElementType());
        Assertions.assertEquals("16", getParam(scoreField.getTypeParamsList(), "max_capacity"));

        FieldSchema embeddingField = structSchema.getFields(1);
        Assertions.assertEquals("embedding", embeddingField.getName());
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfVector, embeddingField.getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.FloatVector, embeddingField.getElementType());
        Assertions.assertEquals("4", getParam(embeddingField.getTypeParamsList(), "dim"));
        Assertions.assertEquals("16", getParam(embeddingField.getTypeParamsList(), "max_capacity"));
    }

    @Test
    void testAddCollectionStructFieldRejectsNullableFalse() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .collectionName("test")
                .fieldName("metadata")
                .maxCapacity(16)
                .nullable(Boolean.FALSE)
                .addStructField(AddFieldReq.builder()
                        .fieldName("score")
                        .dataType(DataType.Float)
                        .build())
                .build();

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addCollectionStructField(request));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddCollectionStructFieldWrapsStructValidationError() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .collectionName("test")
                .fieldName("metadata")
                .maxCapacity(16)
                .addStructField(AddFieldReq.builder()
                        .fieldName("score")
                        .dataType(DataType.Float)
                        .isNullable(Boolean.TRUE)
                        .build())
                .build();

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class, request::toStructFieldSchema);
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("cannot be nullable"));
    }

    @Test
    void testStructFieldSchemaNormalizesNullNullable() {
        CreateCollectionReq.StructFieldSchema structFieldSchema = CreateCollectionReq.StructFieldSchema.builder()
                .name("metadata")
                .maxCapacity(16)
                .nullable(null)
                .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("score")
                        .dataType(DataType.Float)
                        .build()))
                .build();

        Assertions.assertFalse(structFieldSchema.getNullable());

        structFieldSchema.setNullable(null);
        Assertions.assertFalse(structFieldSchema.getNullable());

        StructArrayFieldSchema grpcStructSchema = SchemaUtils.convertToGrpcStructFieldSchema(structFieldSchema);
        Assertions.assertFalse(grpcStructSchema.getNullable());
    }

    @Test
    void testDropCollection() {
        DropCollectionReq req = DropCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.dropCollection(req);
    }

    @Test
    void testTruncateCollection() {
        TruncateCollectionReq req = TruncateCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.truncateCollection(req);
    }

    @Test
    void testHasCollection() {
        HasCollectionReq req = HasCollectionReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.hasCollection(req);
    }

    @Test
    void testDescribeCollection() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeCollectionById() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionId(123456L)
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testDescribeCollectionByNameAndId() {
        DescribeCollectionReq req = DescribeCollectionReq.builder()
                .collectionName("test2")
                .collectionId(123456L)
                .build();
        DescribeCollectionResp resp = client_v2.describeCollection(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testBatchDescribeCollectionsByNames() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionNames(Arrays.asList("test", "test2"))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testBatchDescribeCollectionsByIds() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionIds(Arrays.asList(123456L, 789012L))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testBatchDescribeCollectionsByNamesAndIds() {
        BatchDescribeCollectionReq req = BatchDescribeCollectionReq.builder()
                .collectionNames(Collections.singletonList("test"))
                .collectionIds(Collections.singletonList(789012L))
                .build();
        List<DescribeCollectionResp> resps = client_v2.batchDescribeCollection(req);
        logger.info("resp: {}", resps);
    }

    @Test
    void testRenameCollection() {
        RenameCollectionReq req = RenameCollectionReq.builder()
                .collectionName("test2")
                .newCollectionName("test")
                .build();
        client_v2.renameCollection(req);
    }

    @Test
    void testLoadCollection() {
        LoadCollectionReq req = LoadCollectionReq.builder()
                .collectionName("test")
                .build();
        client_v2.loadCollection(req);

    }

    @Test
    void testReleaseCollection() {
        ReleaseCollectionReq req = ReleaseCollectionReq.builder()
                .collectionName("test")
                .async(Boolean.FALSE)
                .build();
        client_v2.releaseCollection(req);
    }

    @Test
    void testGetLoadState() {
        GetLoadStateReq req = GetLoadStateReq.builder()
                .collectionName("test")
                .build();
        Boolean resp = client_v2.getLoadState(req);
        logger.info("resp: {}", resp);
    }

    @Test
    void testGetCollectionStats() {
        GetCollectionStatsReq req = GetCollectionStatsReq.builder()
                .collectionName("test")
                .build();
        GetCollectionStatsResp resp = client_v2.getCollectionStats(req);
    }

}
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
import io.milvus.param.Constant;
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

    private AddFunctionFieldReq.AddFunctionFieldReqBuilder addFunctionFieldBuilder() {
        return AddFunctionFieldReq.builder()
                .collectionName("test")
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .function(CreateCollectionReq.Function.builder()
                        .name("bm25")
                        .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                        .inputFieldNames(Collections.singletonList("text"))
                        .outputFieldNames(Collections.singletonList("sparse"))
                        .build());
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
                .indexParam(IndexParam.builder()
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
                .addStructField(AddFieldReq.builder()
                        .fieldName("binary_embedding")
                        .dataType(DataType.BinaryVector)
                        .dimension(32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("float16_embedding")
                        .dataType(DataType.Float16Vector)
                        .dimension(16)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("bfloat16_embedding")
                        .dataType(DataType.BFloat16Vector)
                        .dimension(16)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("int8_embedding")
                        .dataType(DataType.Int8Vector)
                        .dimension(16)
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

        FieldSchema binaryEmbeddingField = structSchema.getFields(2);
        Assertions.assertEquals("binary_embedding", binaryEmbeddingField.getName());
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfVector, binaryEmbeddingField.getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.BinaryVector, binaryEmbeddingField.getElementType());
        Assertions.assertEquals("32", getParam(binaryEmbeddingField.getTypeParamsList(), "dim"));
        Assertions.assertEquals("16", getParam(binaryEmbeddingField.getTypeParamsList(), "max_capacity"));

        FieldSchema float16EmbeddingField = structSchema.getFields(3);
        Assertions.assertEquals("float16_embedding", float16EmbeddingField.getName());
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfVector, float16EmbeddingField.getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.Float16Vector, float16EmbeddingField.getElementType());
        Assertions.assertEquals("16", getParam(float16EmbeddingField.getTypeParamsList(), "dim"));
        Assertions.assertEquals("16", getParam(float16EmbeddingField.getTypeParamsList(), "max_capacity"));

        FieldSchema bfloat16EmbeddingField = structSchema.getFields(4);
        Assertions.assertEquals("bfloat16_embedding", bfloat16EmbeddingField.getName());
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfVector, bfloat16EmbeddingField.getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.BFloat16Vector, bfloat16EmbeddingField.getElementType());
        Assertions.assertEquals("16", getParam(bfloat16EmbeddingField.getTypeParamsList(), "dim"));
        Assertions.assertEquals("16", getParam(bfloat16EmbeddingField.getTypeParamsList(), "max_capacity"));

        FieldSchema int8EmbeddingField = structSchema.getFields(5);
        Assertions.assertEquals("int8_embedding", int8EmbeddingField.getName());
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfVector, int8EmbeddingField.getDataType());
        Assertions.assertEquals(io.milvus.grpc.DataType.Int8Vector, int8EmbeddingField.getElementType());
        Assertions.assertEquals("16", getParam(int8EmbeddingField.getTypeParamsList(), "dim"));
        Assertions.assertEquals("16", getParam(int8EmbeddingField.getTypeParamsList(), "max_capacity"));
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
    void testAddCollectionField() {
        client_v2.addCollectionField(AddCollectionFieldReq.builder()
                .databaseName("default")
                .collectionName("test")
                .fieldName("new_field")
                .description("new nullable field")
                .dataType(DataType.VarChar)
                .maxLength(128)
                .isNullable(true)
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        io.milvus.grpc.AlterCollectionSchemaRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals(1, rpcRequest.getAction().getAddRequest().getFieldInfosCount());
        Assertions.assertEquals(0, rpcRequest.getAction().getAddRequest().getFuncSchemaCount());

        io.milvus.grpc.AlterCollectionSchemaRequest.FieldInfo fieldInfo =
                rpcRequest.getAction().getAddRequest().getFieldInfos(0);
        Assertions.assertEquals("new_field", fieldInfo.getFieldSchema().getName());
        Assertions.assertEquals("new nullable field", fieldInfo.getFieldSchema().getDescription());
        Assertions.assertEquals(io.milvus.grpc.DataType.VarChar, fieldInfo.getFieldSchema().getDataType());
        Assertions.assertTrue(fieldInfo.getFieldSchema().getNullable());
        Assertions.assertEquals("128", getParam(fieldInfo.getFieldSchema().getTypeParamsList(), "max_length"));
        Assertions.assertTrue(fieldInfo.getIndexName().isEmpty());
        Assertions.assertEquals(0, fieldInfo.getExtraParamsCount());
    }

    @Test
    void testDropCollectionFieldByName() {
        client_v2.dropCollectionField(DropCollectionFieldReq.builder()
                .databaseName("default")
                .collectionName("test")
                .fieldName("text")
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        io.milvus.grpc.AlterCollectionSchemaRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("text", rpcRequest.getAction().getDropRequest().getFieldName());
        Assertions.assertFalse(rpcRequest.getAction().getDropRequest().hasFieldId());
    }

    @Test
    void testDropCollectionFieldById() {
        client_v2.dropCollectionField(DropCollectionFieldReq.builder()
                .collectionName("test")
                .fieldId(100L)
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        io.milvus.grpc.AlterCollectionSchemaRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals(100L, rpcRequest.getAction().getDropRequest().getFieldId());
        Assertions.assertFalse(rpcRequest.getAction().getDropRequest().hasFieldName());
    }

    @Test
    void testDropCollectionFieldRejectsMissingIdentifier() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.dropCollectionField(DropCollectionFieldReq.builder()
                        .collectionName("test")
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testDropCollectionFieldRejectsMultipleIdentifiers() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.dropCollectionField(DropCollectionFieldReq.builder()
                        .collectionName("test")
                        .fieldName("text")
                        .fieldId(100L)
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionField() {
        IndexParam indexParam = IndexParam.builder()
                .fieldName("sparse")
                .indexName("sparse_idx")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25)
                .extraParams(Collections.singletonMap("drop_ratio_build", 0.2))
                .build();
        AddFunctionFieldReq request = addFunctionFieldBuilder()
                .databaseName("default")
                .build();
        request.setIndexParam(indexParam);
        Assertions.assertSame(indexParam, request.getIndexParam());

        client_v2.addFunctionField(request);

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        io.milvus.grpc.AlterCollectionSchemaRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals(1, rpcRequest.getAction().getAddRequest().getFieldInfosCount());
        Assertions.assertEquals(1, rpcRequest.getAction().getAddRequest().getFuncSchemaCount());
        Assertions.assertEquals("sparse", rpcRequest.getAction().getAddRequest().getFieldInfos(0).getFieldSchema().getName());
        Assertions.assertEquals(io.milvus.grpc.DataType.SparseFloatVector,
                rpcRequest.getAction().getAddRequest().getFieldInfos(0).getFieldSchema().getDataType());
        Assertions.assertTrue(rpcRequest.getAction().getAddRequest().getFieldInfos(0).getFieldSchema().getIsFunctionOutput());
        Assertions.assertFalse(rpcRequest.getAction().getAddRequest().getFieldInfos(0).getFieldSchema().getNullable());
        Assertions.assertEquals("sparse_idx", rpcRequest.getAction().getAddRequest().getFieldInfos(0).getIndexName());
        Assertions.assertEquals("SPARSE_INVERTED_INDEX",
                getParam(rpcRequest.getAction().getAddRequest().getFieldInfos(0).getExtraParamsList(), Constant.INDEX_TYPE));
        Assertions.assertEquals("BM25",
                getParam(rpcRequest.getAction().getAddRequest().getFieldInfos(0).getExtraParamsList(), Constant.METRIC_TYPE));
        Assertions.assertEquals("0.2",
                getParam(rpcRequest.getAction().getAddRequest().getFieldInfos(0).getExtraParamsList(), "drop_ratio_build"));
        Assertions.assertEquals("bm25", rpcRequest.getAction().getAddRequest().getFuncSchema(0).getName());
        Assertions.assertEquals("text", rpcRequest.getAction().getAddRequest().getFuncSchema(0).getInputFieldNames(0));
        Assertions.assertEquals("sparse", rpcRequest.getAction().getAddRequest().getFuncSchema(0).getOutputFieldNames(0));
    }

    @Test
    void testAddFunctionFieldRejectsNullFunction() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder().function(null).build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionFieldRejectsMismatchedOutputField() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .function(CreateCollectionReq.Function.builder()
                                .name("bm25")
                                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                                .inputFieldNames(Collections.singletonList("text"))
                                .outputFieldNames(Collections.singletonList("other"))
                                .build())
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionFieldRejectsMissingBoundIndex() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder().build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionFieldRejectsAutoIndex() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .indexParam(IndexParam.builder().fieldName("sparse").build())
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionFieldRejectsMismatchedBoundIndexField() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .indexParam(IndexParam.builder()
                                .fieldName("other")
                                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                                .build())
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionFieldRejectsDuplicateIndexType() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .indexParam(IndexParam.builder()
                                .fieldName("sparse")
                                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                                .extraParams(Collections.singletonMap(Constant.INDEX_TYPE, "SPARSE_WAND"))
                                .build())
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testDropCollectionFunction() {
        client_v2.dropCollectionFunction(DropCollectionFunctionReq.builder()
                .databaseName("default")
                .collectionName("test")
                .functionName("bm25")
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        io.milvus.grpc.AlterCollectionSchemaRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("bm25", rpcRequest.getAction().getDropRequest().getFunctionName());
        Assertions.assertFalse(rpcRequest.getAction().getDropRequest().getDropFunctionOutputFields());
    }

    @Test
    void testDropFunctionField() {
        client_v2.dropFunctionField(DropFunctionFieldReq.builder()
                .databaseName("default")
                .collectionName("test")
                .functionName("bm25")
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        io.milvus.grpc.AlterCollectionSchemaRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("bm25", rpcRequest.getAction().getDropRequest().getFunctionName());
        Assertions.assertTrue(rpcRequest.getAction().getDropRequest().getDropFunctionOutputFields());
    }

    @Test
    void testDropFunctionFieldRejectsEmptyFunctionName() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.dropFunctionField(DropFunctionFieldReq.builder()
                        .collectionName("test")
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testGetCollectionStats() {
        GetCollectionStatsReq req = GetCollectionStatsReq.builder()
                .collectionName("test")
                .build();
        GetCollectionStatsResp resp = client_v2.getCollectionStats(req);
        Assertions.assertEquals(10L, resp.getNumOfEntities());
        Assertions.assertEquals("10", resp.getStats().get("row_count"));
    }

}

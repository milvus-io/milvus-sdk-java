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

package io.milvus.integration.v2.service.collection;

import io.milvus.grpc.AddCollectionFieldRequest;
import io.milvus.grpc.DropCollectionFunctionRequest;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.param.Constant;
import io.milvus.support.v2.BaseTest;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Tag("integration")
class CollectionAddFieldTest extends BaseTest {

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
        verify(blockingStub, never()).addCollectionField(any());
    }

    @Test
    void testAddCollectionFieldFallsBackForOlderServer() throws Exception {
        when(blockingStub.alterCollectionSchema(any()))
                .thenThrow(io.grpc.Status.UNIMPLEMENTED.asRuntimeException());
        when(blockingStub.addCollectionField(any()))
                .thenReturn(io.milvus.grpc.Status.newBuilder()
                        .setErrorCode(io.milvus.grpc.ErrorCode.RateLimit)
                        .setReason("rate limited")
                        .build())
                .thenReturn(io.milvus.grpc.Status.newBuilder().setCode(0).build());

        client_v2.addCollectionField(AddCollectionFieldReq.builder()
                .databaseName("default")
                .collectionName("test")
                .fieldName("new_field")
                .description("new nullable field")
                .dataType(DataType.VarChar)
                .maxLength(128)
                .isNullable(true)
                .build());

        verify(blockingStub).alterCollectionSchema(any());
        ArgumentCaptor<AddCollectionFieldRequest> captor =
                ArgumentCaptor.forClass(AddCollectionFieldRequest.class);
        verify(blockingStub, times(2)).addCollectionField(captor.capture());
        AddCollectionFieldRequest rpcRequest = captor.getAllValues().get(0);
        FieldSchema fieldSchema = FieldSchema.parseFrom(rpcRequest.getSchema());
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("new_field", fieldSchema.getName());
        Assertions.assertEquals("new nullable field", fieldSchema.getDescription());
        Assertions.assertEquals(io.milvus.grpc.DataType.VarChar, fieldSchema.getDataType());
        Assertions.assertTrue(fieldSchema.getNullable());
        Assertions.assertEquals("128", getParam(fieldSchema.getTypeParamsList(), "max_length"));
    }

    @Test
    void testAddCollectionFieldDoesNotFallbackForOtherRpcErrors() {
        when(blockingStub.alterCollectionSchema(any()))
                .thenThrow(io.grpc.Status.PERMISSION_DENIED.asRuntimeException());

        assertThrows(MilvusClientException.class, () ->
                client_v2.addCollectionField(AddCollectionFieldReq.builder()
                        .collectionName("test")
                        .fieldName("new_field")
                        .dataType(DataType.VarChar)
                        .maxLength(128)
                        .isNullable(true)
                        .build()));

        verify(blockingStub, never()).addCollectionField(any());
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
    void testAddFunctionFieldRejectsNullOutputFieldNames() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .function(CreateCollectionReq.Function.builder()
                                .name("bm25")
                                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                                .inputFieldNames(Collections.singletonList("text"))
                                .outputFieldNames(null)
                                .build())
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertEquals("Function output field names cannot be null.", exception.getMessage());
        verify(blockingStub, never()).alterCollectionSchema(any());
    }

    @Test
    void testAddFunctionFieldLeavesOutputFieldValidationToServer() {
        client_v2.addFunctionField(addFunctionFieldBuilder()
                .function(CreateCollectionReq.Function.builder()
                        .name("bm25")
                        .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                        .inputFieldNames(Collections.singletonList("text"))
                        .outputFieldNames(Collections.singletonList("other"))
                        .build())
                .indexParam(IndexParam.builder()
                        .fieldName("sparse")
                        .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                        .build())
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        Assertions.assertEquals("other",
                captor.getValue().getAction().getAddRequest().getFuncSchema(0).getOutputFieldNames(0));
    }

    @Test
    void testAddFunctionFieldRejectsUnsupportedFunctionType() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .function(CreateCollectionReq.Function.builder()
                                .name("embedding")
                                .functionType(io.milvus.common.clientenum.FunctionType.TEXTEMBEDDING)
                                .inputFieldNames(Collections.singletonList("text"))
                                .outputFieldNames(Collections.singletonList("sparse"))
                                .build())
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionFieldRejectsBm25WithWrongOutputType() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .dataType(DataType.BinaryVector)
                        .build()));
        Assertions.assertEquals(io.milvus.v2.exception.ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testAddFunctionFieldSupportsMinhashWithBinaryOutput() {
        client_v2.addFunctionField(AddFunctionFieldReq.builder()
                .collectionName("test")
                .fieldName("minhash")
                .dataType(DataType.BinaryVector)
                .dimension(512)
                .function(CreateCollectionReq.Function.builder()
                        .name("minhash")
                        .functionType(io.milvus.common.clientenum.FunctionType.MINHASH)
                        .inputFieldNames(Collections.singletonList("text"))
                        .outputFieldNames(Collections.singletonList("minhash"))
                        .build())
                .indexParam(IndexParam.builder()
                        .fieldName("minhash")
                        .indexType(IndexParam.IndexType.MINHASH_LSH)
                        .metricType(IndexParam.MetricType.MHJACCARD)
                        .build())
                .build());

        verify(blockingStub).alterCollectionSchema(any());
    }

    @Test
    void testAddFunctionFieldRejectsMinhashWithWrongOutputType() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .function(CreateCollectionReq.Function.builder()
                                .name("minhash")
                                .functionType(io.milvus.common.clientenum.FunctionType.MINHASH)
                                .inputFieldNames(Collections.singletonList("text"))
                                .outputFieldNames(Collections.singletonList("sparse"))
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
    void testAddFunctionFieldAcceptsAutoIndex() {
        client_v2.addFunctionField(addFunctionFieldBuilder()
                .indexParam(IndexParam.builder().fieldName("sparse").build())
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        Assertions.assertEquals("AUTOINDEX",
                getParam(captor.getValue().getAction().getAddRequest().getFieldInfos(0).getExtraParamsList(),
                        Constant.INDEX_TYPE));
    }

    @Test
    void testAddFunctionFieldAcceptsEmptyBoundIndexField() {
        client_v2.addFunctionField(addFunctionFieldBuilder()
                .indexParam(IndexParam.builder()
                        .fieldName("")
                        .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                        .build())
                .build());

        verify(blockingStub).alterCollectionSchema(any());
    }

    @Test
    void testAddFunctionFieldRejectsNoneIndexType() {
        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> client_v2.addFunctionField(addFunctionFieldBuilder()
                        .indexParam(IndexParam.builder()
                                .fieldName("sparse")
                                .indexType(IndexParam.IndexType.None)
                                .build())
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
        Assertions.assertEquals("Bound index field must be empty or match the field being added.",
                exception.getMessage());
    }

    @Test
    void testAddFunctionFieldTypedIndexParamsOverrideDuplicates() {
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put(Constant.INDEX_TYPE, "SPARSE_WAND");
        extraParams.put(Constant.METRIC_TYPE, "IP");
        extraParams.put("drop_ratio_build", 0.2);
        client_v2.addFunctionField(addFunctionFieldBuilder()
                .indexParam(IndexParam.builder()
                        .fieldName("sparse")
                        .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                        .metricType(IndexParam.MetricType.BM25)
                        .extraParams(extraParams)
                        .build())
                .build());

        ArgumentCaptor<io.milvus.grpc.AlterCollectionSchemaRequest> captor =
                ArgumentCaptor.forClass(io.milvus.grpc.AlterCollectionSchemaRequest.class);
        verify(blockingStub).alterCollectionSchema(captor.capture());
        List<KeyValuePair> params =
                captor.getValue().getAction().getAddRequest().getFieldInfos(0).getExtraParamsList();
        Assertions.assertEquals("SPARSE_INVERTED_INDEX", getParam(params, Constant.INDEX_TYPE));
        Assertions.assertEquals("BM25", getParam(params, Constant.METRIC_TYPE));
        Assertions.assertEquals("0.2", getParam(params, "drop_ratio_build"));
        Assertions.assertEquals(1,
                params.stream().filter(param -> Constant.INDEX_TYPE.equals(param.getKey())).count());
        Assertions.assertEquals(1,
                params.stream().filter(param -> Constant.METRIC_TYPE.equals(param.getKey())).count());
    }

    @Test
    void testDropCollectionFunction() {
        client_v2.dropCollectionFunction(DropCollectionFunctionReq.builder()
                .databaseName("default")
                .collectionName("test")
                .functionName("bm25")
                .build());

        ArgumentCaptor<DropCollectionFunctionRequest> captor =
                ArgumentCaptor.forClass(DropCollectionFunctionRequest.class);
        verify(blockingStub).dropCollectionFunction(captor.capture());
        DropCollectionFunctionRequest rpcRequest = captor.getValue();
        Assertions.assertEquals("default", rpcRequest.getDbName());
        Assertions.assertEquals("test", rpcRequest.getCollectionName());
        Assertions.assertEquals("bm25", rpcRequest.getFunctionName());
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
}

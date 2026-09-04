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

package io.milvus.unit.v2.utils;
import io.milvus.v2.utils.DataUtils;

import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.v2.common.DataType;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Tag("unit")
class DataUtilsFieldOperationTest {

    @Test
    void testFieldOperationConversion() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        JsonObject row = row(1L, true, false);
        row.add("values", JsonUtils.toJsonTree(Arrays.asList(1L, 2L)));
        List<UpsertReq.FieldPartialUpdateOp> operations = Arrays.asList(
                UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("values")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.REPLACE)
                        .build(),
                UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("values")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)
                        .build(),
                UpsertReq.FieldPartialUpdateOp.builder()
                        .fieldName("values")
                        .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_REMOVE)
                        .build());

        UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .data(Collections.singletonList(row))
                        .partialUpdate(true)
                        .fieldOps(operations)
                        .build(),
                collection);

        Assertions.assertEquals(Arrays.asList(
                        FieldPartialUpdateOp.OpType.REPLACE,
                        FieldPartialUpdateOp.OpType.ARRAY_APPEND,
                        FieldPartialUpdateOp.OpType.ARRAY_REMOVE),
                request.getFieldOpsList().stream().map(FieldPartialUpdateOp::getOp).collect(Collectors.toList()));
    }

    @Test
    void testNonReplaceFieldOperationEnablesPartialUpdate() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));

        for (UpsertReq.FieldPartialUpdateOp.OpType opType : Arrays.asList(
                UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND,
                UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_REMOVE)) {
            UpsertReq upsertReq = UpsertReq.builder()
                    .collectionName("test")
                    .data(Collections.singletonList(row))
                    .fieldOps(Collections.singletonList(UpsertReq.FieldPartialUpdateOp.builder()
                            .fieldName("values")
                            .opType(opType)
                            .build()))
                    .build();
            Assertions.assertTrue(upsertReq.isPartialUpdate(), "Expected builder promotion for " + opType);

            UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                    upsertReq,
                    collection);

            Assertions.assertTrue(request.getPartialUpdate(), "Expected partial update for " + opType);
            Assertions.assertEquals(Arrays.asList("id", "values"), fieldNames(request.getFieldsDataList()));
        }
    }

    @Test
    void testReplaceOnlyFieldOperationFollowsPartialUpdateFlag() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        UpsertReq.FieldPartialUpdateOp replace = UpsertReq.FieldPartialUpdateOp.builder()
                .fieldName("values")
                .opType(UpsertReq.FieldPartialUpdateOp.OpType.REPLACE)
                .build();

        JsonObject fullRow = row(1L, true, false);
        fullRow.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));
        UpsertReq fullUpsertReq = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(fullRow))
                .fieldOps(Collections.singletonList(replace))
                .build();
        Assertions.assertFalse(fullUpsertReq.isPartialUpdate());
        UpsertRequest fullRequest = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                fullUpsertReq,
                collection);
        Assertions.assertFalse(fullRequest.getPartialUpdate());
        Assertions.assertEquals(Arrays.asList("id", "vector", "values"),
                fieldNames(fullRequest.getFieldsDataList()));

        JsonObject partialRow = new JsonObject();
        partialRow.addProperty("id", 1L);
        partialRow.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));
        UpsertReq partialUpsertReq = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(partialRow))
                .partialUpdate(true)
                .fieldOps(Collections.singletonList(replace))
                .build();
        Assertions.assertTrue(partialUpsertReq.isPartialUpdate());
        UpsertRequest partialRequest = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                partialUpsertReq,
                collection);
        Assertions.assertTrue(partialRequest.getPartialUpdate());
        Assertions.assertEquals(Arrays.asList("id", "values"),
                fieldNames(partialRequest.getFieldsDataList()));
    }

    @Test
    void testPostBuildFieldOperationMutationPreservesPartialUpdateInvariant() {
        DescribeCollectionResp collection = describeCollectionWithArrayField();
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("values", JsonUtils.toJsonTree(Collections.singletonList(3L)));
        UpsertReq request = UpsertReq.builder()
                .collectionName("test")
                .data(Collections.singletonList(row))
                .build();

        request.setFieldOps(Collections.singletonList(UpsertReq.FieldPartialUpdateOp.builder()
                .fieldName("values")
                .opType(UpsertReq.FieldPartialUpdateOp.OpType.ARRAY_APPEND)
                .build()));
        Assertions.assertTrue(request.isPartialUpdate());

        request.setPartialUpdate(false);
        Assertions.assertTrue(request.isPartialUpdate());

        UpsertRequest grpcRequest = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                request,
                collection);
        Assertions.assertTrue(grpcRequest.getPartialUpdate());
        Assertions.assertEquals(Arrays.asList("id", "values"), fieldNames(grpcRequest.getFieldsDataList()));
    }

    @Test
    void testRejectsInvalidFieldOperations() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(1L, true, false);

        List<UpsertReq.FieldPartialUpdateOp> invalidOperations = Arrays.asList(
                null,
                UpsertReq.FieldPartialUpdateOp.builder().fieldName("").build(),
                UpsertReq.FieldPartialUpdateOp.builder().fieldName("vector").opType(null).build());
        for (UpsertReq.FieldPartialUpdateOp operation : invalidOperations) {
            MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                            UpsertReq.builder()
                                    .collectionName("test")
                                    .data(Collections.singletonList(row))
                                    .fieldOps(Collections.singletonList(operation))
                                    .build(),
                            collection));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        }
    }

    private static DescribeCollectionResp describeCollection(boolean autoId, boolean withFunctionOutput,
                                                             boolean withStructField) {
        CreateCollectionReq.FieldSchema id = CreateCollectionReq.FieldSchema.builder()
                .name("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(autoId)
                .build();
        CreateCollectionReq.FieldSchema vector = CreateCollectionReq.FieldSchema.builder()
                .name("vector")
                .dataType(DataType.FloatVector)
                .dimension(2)
                .build();

        CreateCollectionReq.CollectionSchema.CollectionSchemaBuilder schemaBuilder =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(Arrays.asList(id, vector))
                        .enableDynamicField(false);

        if (withFunctionOutput) {
            CreateCollectionReq.FieldSchema embedding = CreateCollectionReq.FieldSchema.builder()
                    .name("embedding")
                    .dataType(DataType.FloatVector)
                    .dimension(2)
                    .build();
            schemaBuilder.fieldSchemaList(Arrays.asList(id, vector, embedding));
            schemaBuilder.functionList(Collections.singletonList(
                    CreateCollectionReq.Function.builder()
                            .outputFieldNames(Collections.singletonList("embedding"))
                            .build()));
        }

        if (withStructField) {
            CreateCollectionReq.StructFieldSchema metadata = CreateCollectionReq.StructFieldSchema.builder()
                    .name("metadata")
                    .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                            .name("score")
                            .dataType(DataType.Float)
                            .build()))
                    .maxCapacity(10)
                    .build();
            schemaBuilder.structFields(Collections.singletonList(metadata));
        }

        return DescribeCollectionResp.builder()
                .collectionName("test")
                .collectionSchema(schemaBuilder.build())
                .build();
    }

    private static DescribeCollectionResp describeCollectionWithArrayField() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        List<CreateCollectionReq.FieldSchema> fields =
                new ArrayList<>(collection.getCollectionSchema().getFieldSchemaList());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("values")
                .dataType(DataType.Array)
                .elementType(DataType.Int64)
                .maxCapacity(10)
                .build());
        collection.getCollectionSchema().setFieldSchemaList(fields);
        return collection;
    }

    private static JsonObject row(Long id, boolean withVector, boolean withEmbedding) {
        JsonObject row = new JsonObject();
        if (id != null) {
            row.addProperty("id", id);
        }
        if (withVector) {
            row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        }
        if (withEmbedding) {
            row.add("embedding", JsonUtils.toJsonTree(Arrays.asList(3.0f, 4.0f)));
        }
        return row;
    }

    private static List<String> fieldNames(List<io.milvus.grpc.FieldData> fieldsData) {
        return fieldsData.stream().map(io.milvus.grpc.FieldData::getFieldName).collect(Collectors.toList());
    }
}

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

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.v2.common.DataType;
import io.milvus.v2.exception.DataNotMatchException;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Tag("unit")
class DataUtilsUpsertValidationTest {

    @Test
    void testFullUpsertRequiresAutoIdField() {
        DescribeCollectionResp collection = describeCollection(true, false, false);
        JsonObject row = row(null, true, false);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("id"));
    }

    @Test
    void testPartialUpsertRequiresAutoIdField() {
        DescribeCollectionResp collection = describeCollection(true, false, false);
        JsonObject row = row(null, true, false);

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Collections.singletonList(row))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("primary key"));
        Assertions.assertTrue(exception.getMessage().contains("id"));
    }

    @Test
    void testPartialUpsertRequiresNonAutoIdPrimaryKey() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject row = row(null, true, false);

        DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Collections.singletonList(row))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("primary key"));
        Assertions.assertTrue(exception.getMessage().contains("id"));
    }

    @Test
    void testPartialUpsertAllowsOmittedStructField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);

        UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .data(Collections.singletonList(row))
                        .partialUpdate(true)
                        .build(),
                collection);

        Assertions.assertEquals(Arrays.asList("id", "vector"), fieldNames(request.getFieldsDataList()));
    }

    @Test
    void testPartialUpsertRejectsInconsistentFieldCounts() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject firstRow = row(1L, true, false);
        JsonObject secondRow = row(2L, false, false);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Arrays.asList(firstRow, secondRow))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
    }

    @Test
    void testPartialUpsertRejectsFieldCountsShorterThanBatchRegardlessOfRowOrder() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        JsonObject populatedRow = row(1L, true, false);
        JsonObject rowWithoutVector = new JsonObject();
        rowWithoutVector.addProperty("id", 2L);

        List<List<JsonObject>> batches = Arrays.asList(
                Arrays.asList(populatedRow, rowWithoutVector),
                Arrays.asList(rowWithoutVector, populatedRow));
        for (List<JsonObject> batch : batches) {
            MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                            UpsertReq.builder()
                                    .collectionName("test")
                                    .data(batch)
                                    .partialUpdate(true)
                                    .build(),
                            collection));

            Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
            Assertions.assertTrue(exception.getMessage().contains("number of values"));
        }
    }

    @Test
    void testFullUpsertRejectsMissingStructField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("metadata"));
    }

    @Test
    void testFullUpsertRejectsStructStorageSubFieldWithDynamicField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject row = row(1L, true, false);
        row.add("metadata[score]", JsonUtils.toJsonTree(Collections.singletonList(1.0f)));

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("cannot be used as a top-level field"));
    }

    @Test
    void testPartialUpsertRejectsStructStorageSubFieldWithDynamicField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("metadata[score]", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                        UpsertReq.builder()
                                .collectionName("test")
                                .data(Collections.singletonList(row))
                                .partialUpdate(true)
                                .build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("Partial struct update is unsupported"));
    }

    @Test
    void testPartialUpsertTreatsShortStructSubFieldNameAsDynamicField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().setEnableDynamicField(true);
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.addProperty("score", "dynamic-value");

        UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .data(Collections.singletonList(row))
                        .partialUpdate(true)
                        .build(),
                collection);

        Assertions.assertEquals(Arrays.asList("id", Constant.DYNAMIC_FIELD_NAME),
                fieldNames(request.getFieldsDataList()));
        Assertions.assertTrue(request.getFieldsData(1).getIsDynamic());
    }

    @Test
    void testPartialUpsertNullsNullableStructField() {
        DescribeCollectionResp collection = describeCollectionWithNullableStruct(true, false);
        JsonObject row = row(1L, true, false);
        row.add("metadata", JsonNull.INSTANCE);

        UpsertRequest request = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .data(Collections.singletonList(row))
                        .partialUpdate(true)
                        .build(),
                collection);

        FieldData metadataField = request.getFieldsDataList().stream()
                .filter(field -> field.getFieldName().equals("metadata"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(metadataField);
        FieldData score = metadataField.getStructArrays().getFields(0);
        Assertions.assertEquals(Collections.singletonList(false), score.getValidDataList());
        Assertions.assertFalse(score.hasScalars());
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

    private static DescribeCollectionResp describeCollectionWithNullableStruct(boolean nullable,
                                                                               boolean withVectorSubField) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("id").dataType(DataType.Int64).isPrimaryKey(true).build());
        schema.getFieldSchemaList().add(CreateCollectionReq.FieldSchema.builder()
                .name("vector").dataType(DataType.FloatVector).dimension(2).build());
        CreateCollectionReq.FieldSchema subField;
        if (withVectorSubField) {
            subField = CreateCollectionReq.FieldSchema.builder()
                    .name("embedding").dataType(DataType.FloatVector).dimension(2)
                    .isNullable(nullable).build();
        } else {
            subField = CreateCollectionReq.FieldSchema.builder()
                    .name("score").dataType(DataType.Float)
                    .isNullable(nullable).build();
        }
        schema.getStructFields().add(CreateCollectionReq.StructFieldSchema.builder()
                .name("metadata")
                .fields(Collections.singletonList(subField))
                .maxCapacity(10)
                .nullable(nullable)
                .build());
        return DescribeCollectionResp.builder().collectionName("test").collectionSchema(schema).build();
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

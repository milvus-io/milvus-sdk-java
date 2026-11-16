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

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.v2.common.DataType;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Tag("unit")
class DataUtilsStructFieldTest {

    @Test
    void testPartitionNameIsSerializedForPartitionKeySchema() {
        DescribeCollectionResp collection = describeCollection(false, false, false);
        collection.getCollectionSchema().getFieldSchemaList().get(0).setIsPartitionKey(true);
        JsonObject row = row(1L, true, false);

        InsertRequest insert = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder()
                        .collectionName("test")
                        .partitionName("partition")
                        .data(Collections.singletonList(row))
                        .build(),
                collection);
        Assertions.assertEquals("partition", insert.getPartitionName());

        UpsertRequest upsert = new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                UpsertReq.builder()
                        .collectionName("test")
                        .partitionName("partition")
                        .data(Collections.singletonList(row))
                        .build(),
                collection);
        Assertions.assertEquals("partition", upsert.getPartitionName());
    }

    @Test
    void testRejectsUnexpectedStructSubField() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);
        JsonObject struct = new JsonObject();
        struct.addProperty("score", 1.0f);
        struct.addProperty("extra", "value");
        JsonArray metadata = new JsonArray();
        metadata.add(struct);
        row.add("metadata", metadata);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("unexpected fields"));
    }

    @Test
    void testAcceptsNullStructSubFieldWhenNullable() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().getStructFields().get(0).getFields().get(0).setIsNullable(true);
        JsonObject row = row(1L, true, false);
        JsonObject struct = new JsonObject();
        struct.add("score", JsonNull.INSTANCE);
        JsonArray metadata = new JsonArray();
        metadata.add(struct);
        row.add("metadata", metadata);

        InsertRequest insert = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        // fieldsData: [id, vector, metadata]
        Assertions.assertEquals(3, insert.getFieldsDataCount());
        FieldData metadataField = insert.getFieldsData(2);
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfStruct, metadataField.getType());
        // score sub-field: outer ArrayArray with one per-row cell; the cell's valid_data
        // marks the null element
        FieldData scoreField = metadataField.getStructArrays().getFields(0);
        Assertions.assertEquals(io.milvus.grpc.DataType.Array, scoreField.getType());
        ScalarField rowCell = scoreField.getScalars().getArrayData().getData(0);
        Assertions.assertEquals(Collections.singletonList(Boolean.FALSE), rowCell.getValidDataList());
        Assertions.assertTrue(rowCell.getFloatData().getDataList().isEmpty());
    }

    @Test
    void testElementNullableStructSubFieldCompaction() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        CreateCollectionReq.FieldSchema score = collection.getCollectionSchema().getStructFields().get(0).getFields().get(0);
        score.setIsNullable(true);
        JsonObject row = row(1L, true, false);
        JsonArray metadata = new JsonArray();
        JsonObject s1 = new JsonObject();
        s1.addProperty("score", 1.0f);
        JsonObject s2 = new JsonObject();
        s2.add("score", JsonNull.INSTANCE);
        JsonObject s3 = new JsonObject();
        s3.addProperty("score", 3.0f);
        metadata.add(s1);
        metadata.add(s2);
        metadata.add(s3);
        row.add("metadata", metadata);

        InsertRequest insert = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        FieldData metadataField = insert.getFieldsData(2);
        FieldData scoreField = metadataField.getStructArrays().getFields(0);
        ScalarField rowCell = scoreField.getScalars().getArrayData().getData(0);
        // only non-null element values are stored, validity marks the null position
        Assertions.assertEquals(Arrays.asList(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE),
                rowCell.getValidDataList());
        Assertions.assertEquals(Arrays.asList(1.0f, 3.0f), rowCell.getFloatData().getDataList());
    }

    @Test
    void testRejectsNullStructSubFieldWhenNonNullable() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        JsonObject row = row(1L, true, false);
        JsonObject struct = new JsonObject();
        struct.add("score", JsonNull.INSTANCE);
        JsonArray metadata = new JsonArray();
        metadata.add(struct);
        row.add("metadata", metadata);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("cannot be null"));
    }

    @Test
    void testNestedArrayStructSubFieldEncoding() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        CreateCollectionReq.StructFieldSchema metadata = collection.getCollectionSchema().getStructFields().get(0);
        metadata.getFields().get(0).setIsNullable(false);
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>(metadata.getFields());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("tags")
                .dataType(DataType.Array)
                .elementType(DataType.VarChar)
                .maxCapacity(8)
                .maxLength(16)
                .isNullable(true)
                .elementNullable(true)
                .build());
        metadata.setFields(fields);
        JsonObject row = row(1L, true, false);
        JsonArray metadataArray = new JsonArray();
        JsonObject s1 = new JsonObject();
        s1.addProperty("score", 1.0f);
        JsonArray tags1 = new JsonArray();
        tags1.add("a");
        tags1.add(JsonNull.INSTANCE);
        s1.add("tags", tags1);
        JsonObject s2 = new JsonObject();
        s2.addProperty("score", 2.0f);
        s2.add("tags", JsonNull.INSTANCE);
        JsonObject s3 = new JsonObject();
        s3.addProperty("score", 3.0f);
        JsonArray tags3 = new JsonArray();
        s3.add("tags", tags3);
        metadataArray.add(s1);
        metadataArray.add(s2);
        metadataArray.add(s3);
        row.add("metadata", metadataArray);

        InsertRequest insert = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        FieldData metadataField = insert.getFieldsData(2);
        FieldData tagsField = metadataField.getStructArrays().getFields(1);
        Assertions.assertEquals(io.milvus.grpc.DataType.Array, tagsField.getType());
        // outer array_data element type is ARRAY
        Assertions.assertEquals(io.milvus.grpc.DataType.Array, tagsField.getScalars().getArrayData().getElementType());
        ScalarField rowCell = tagsField.getScalars().getArrayData().getData(0);
        // element-level validity: ["a", null] non-null, null inner array invalid, [] non-null
        Assertions.assertEquals(Arrays.asList(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE), rowCell.getValidDataList());
        Assertions.assertEquals(io.milvus.grpc.DataType.VarChar, rowCell.getArrayData().getElementType());
        // only two non-null inner arrays are stored
        Assertions.assertEquals(2, rowCell.getArrayData().getDataCount());
        // first inner array ["a", null]: compacted to ["a"], leaf validity [true, false]
        ScalarField leaf1 = rowCell.getArrayData().getData(0);
        Assertions.assertEquals(Arrays.asList("a"), leaf1.getStringData().getDataList());
        Assertions.assertEquals(Arrays.asList(Boolean.TRUE, Boolean.FALSE), leaf1.getValidDataList());
        // second inner array []: empty string data, no validity
        ScalarField leaf2 = rowCell.getArrayData().getData(1);
        Assertions.assertTrue(leaf2.getStringData().getDataList().isEmpty());
        Assertions.assertTrue(leaf2.getValidDataList().isEmpty());
    }

    @Test
    void testNestedArrayStructSubFieldRejectsNullInnerArrayWhenNonNullable() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        CreateCollectionReq.StructFieldSchema metadata = collection.getCollectionSchema().getStructFields().get(0);
        metadata.getFields().get(0).setIsNullable(false);
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>(metadata.getFields());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("tags")
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .maxCapacity(8)
                .isNullable(false)
                .elementNullable(false)
                .build());
        metadata.setFields(fields);
        JsonObject row = row(1L, true, false);
        JsonArray metadataArray = new JsonArray();
        JsonObject s1 = new JsonObject();
        s1.addProperty("score", 1.0f);
        s1.add("tags", JsonNull.INSTANCE);
        metadataArray.add(s1);
        row.add("metadata", metadataArray);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("cannot be null")
                || exception.getMessage().contains("does not allow null"));
    }

    @Test
    void testNestedArrayStructSubFieldRejectsNullLeafWhenNonNullable() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        CreateCollectionReq.StructFieldSchema metadata = collection.getCollectionSchema().getStructFields().get(0);
        metadata.getFields().get(0).setIsNullable(false);
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>(metadata.getFields());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("tags")
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .maxCapacity(8)
                .isNullable(true)   // inner array may be null
                .elementNullable(false) // but individual leaf values may not be null
                .build());
        metadata.setFields(fields);
        JsonObject row = row(1L, true, false);
        JsonArray metadataArray = new JsonArray();
        JsonObject s1 = new JsonObject();
        s1.addProperty("score", 1.0f);
        JsonArray tags = new JsonArray();
        tags.add(1);
        tags.add(JsonNull.INSTANCE); // required leaf null
        s1.add("tags", tags);
        metadataArray.add(s1);
        row.add("metadata", metadataArray);

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                        InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                        collection));

        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("does not allow null elements"));
    }

    @Test
    void testElementNullableVectorStructSubFieldEncoding() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        CreateCollectionReq.StructFieldSchema metadata = collection.getCollectionSchema().getStructFields().get(0);
        metadata.getFields().get(0).setIsNullable(false);
        List<CreateCollectionReq.FieldSchema> fields = new ArrayList<>(metadata.getFields());
        fields.add(CreateCollectionReq.FieldSchema.builder()
                .name("emb")
                .dataType(DataType.FloatVector)
                .dimension(2)
                .isNullable(true)
                .build());
        metadata.setFields(fields);
        JsonObject row = row(1L, true, false);
        JsonArray metadataArray = new JsonArray();
        JsonObject s1 = new JsonObject();
        s1.addProperty("score", 1.0f);
        s1.add("emb", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        JsonObject s2 = new JsonObject();
        s2.addProperty("score", 2.0f);
        s2.add("emb", JsonNull.INSTANCE);
        JsonObject s3 = new JsonObject();
        s3.addProperty("score", 3.0f);
        s3.add("emb", JsonUtils.toJsonTree(Arrays.asList(3.0f, 4.0f)));
        metadataArray.add(s1);
        metadataArray.add(s2);
        metadataArray.add(s3);
        row.add("metadata", metadataArray);

        InsertRequest insert = new DataUtils.InsertBuilderWrapper().convertGrpcInsertRequest(
                InsertReq.builder().collectionName("test").data(Collections.singletonList(row)).build(),
                collection);

        FieldData metadataField = insert.getFieldsData(2);
        FieldData embField = metadataField.getStructArrays().getFields(1);
        Assertions.assertEquals(io.milvus.grpc.DataType.ArrayOfVector, embField.getType());
        VectorArray vecArr = embField.getVectors().getVectorArray();
        Assertions.assertEquals(io.milvus.grpc.DataType.FloatVector, vecArr.getElementType());
        Assertions.assertEquals(2, vecArr.getDim());
        Assertions.assertEquals(1, vecArr.getDataCount());
        VectorField rowCell = vecArr.getData(0);
        // only non-null vectors are stored; validity marks the null vector position
        Assertions.assertEquals(Arrays.asList(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE),
                rowCell.getValidDataList());
        Assertions.assertEquals(Arrays.asList(1.0f, 2.0f, 3.0f, 4.0f),
                rowCell.getFloatVector().getDataList());
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
}

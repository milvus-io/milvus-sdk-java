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

import java.util.Arrays;
import java.util.Collections;

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
    void testRejectsNullStructSubFieldEvenWhenNullable() {
        DescribeCollectionResp collection = describeCollection(false, false, true);
        collection.getCollectionSchema().getStructFields().get(0).getFields().get(0).setIsNullable(true);
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

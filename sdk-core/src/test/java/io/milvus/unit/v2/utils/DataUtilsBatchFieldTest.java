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
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.v2.common.DataType;
import io.milvus.v2.exception.DataNotMatchException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

@Tag("unit")
class DataUtilsBatchFieldTest {

    @Test
    void testUpsertRejectsUnsupportedStructSubFieldType() {
        for (DataType subFieldType : new DataType[]{DataType.JSON, DataType.Geometry, DataType.Timestamptz}) {
            DataNotMatchException exception = Assertions.assertThrows(DataNotMatchException.class,
                    () -> new DataUtils.InsertBuilderWrapper().convertGrpcUpsertRequest(
                            UpsertReq.builder().collectionName("test")
                                    .data(Collections.singletonList(structRow(subFieldType)))
                                    .build(),
                            describeCollectionWithStructSubField(subFieldType)));
            Assertions.assertTrue(exception.getMessage().contains("Unsupported element type"));
        }
    }

    private static DescribeCollectionResp describeCollectionWithStructSubField(DataType subFieldType) {
        CreateCollectionReq.StructFieldSchema metadata = CreateCollectionReq.StructFieldSchema.builder()
                .name("metadata")
                .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("geo")
                        .dataType(subFieldType)
                        .build()))
                .maxCapacity(10)
                .build();
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(Arrays.asList(
                        CreateCollectionReq.FieldSchema.builder().name("id").dataType(DataType.Int64).isPrimaryKey(true).build(),
                        CreateCollectionReq.FieldSchema.builder().name("vector").dataType(DataType.FloatVector).dimension(2).build()))
                .structFields(Collections.singletonList(metadata))
                .enableDynamicField(false)
                .build();
        return DescribeCollectionResp.builder()
                .collectionName("test")
                .collectionSchema(schema)
                .build();
    }

    private static JsonObject structRow(DataType subFieldType) {
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.add("vector", JsonUtils.toJsonTree(Arrays.asList(1.0f, 2.0f)));
        JsonArray metadataArray = new JsonArray();
        JsonObject item = new JsonObject();
        if (subFieldType == DataType.Geometry) {
            item.addProperty("geo", "POINT(1 1)");
        } else if (subFieldType == DataType.Timestamptz) {
            item.addProperty("geo", "2026-01-01T00:00:00Z");
        } else {
            item.add("geo", JsonUtils.toJsonTree(Collections.singletonMap("k", 1)));
        }
        metadataArray.add(item);
        row.add("metadata", metadataArray);
        return row;
    }
}

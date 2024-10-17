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

package io.milvus.bulkwriter;

import io.milvus.bulkwriter.common.utils.V2AdapterUtils;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkWriterTest {
    @Test
    void testV2AdapterUtils() {
        CreateCollectionReq.CollectionSchema schemaV2 = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("bool_field")
                .dataType(DataType.Bool)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int8_field")
                .dataType(DataType.Int8)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int16_field")
                .dataType(DataType.Int16)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int32_field")
                .dataType(DataType.Int32)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int64_field")
                .dataType(DataType.Int64)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float_field")
                .dataType(DataType.Float)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("double_field")
                .dataType(DataType.Double)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("varchar_field")
                .dataType(DataType.VarChar)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("json_field")
                .dataType(DataType.JSON)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("arr_int_field")
                .dataType(DataType.Array)
                .maxCapacity(50)
                .elementType(DataType.Int32)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("arr_float_field")
                .dataType(DataType.Array)
                .maxCapacity(20)
                .elementType(DataType.Float)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("arr_varchar_field")
                .dataType(DataType.Array)
                .maxCapacity(10)
                .elementType(DataType.VarChar)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float_vector_field")
                .dataType(DataType.FloatVector)
                .dimension(128)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("binary_vector_field")
                .dataType(DataType.BinaryVector)
                .dimension(512)
                .build());

        CollectionSchemaParam schemaV1 = V2AdapterUtils.convertV2Schema(schemaV2);
        Assertions.assertEquals(schemaV2.isEnableDynamicField(), schemaV1.isEnableDynamicField());

        List<CreateCollectionReq.FieldSchema> fieldSchemaListV2 = schemaV2.getFieldSchemaList();
        Map<String, CreateCollectionReq.FieldSchema> fieldSchemaMapV2 = new HashMap<>();
        for (CreateCollectionReq.FieldSchema field : fieldSchemaListV2) {
            fieldSchemaMapV2.put(field.getName(), field);
        }

        List<FieldType> fieldSchemaListV1 = schemaV1.getFieldTypes();
        for (FieldType fieldSchemaV1 : fieldSchemaListV1) {
            Assertions.assertTrue(fieldSchemaMapV2.containsKey(fieldSchemaV1.getName()));
            CreateCollectionReq.FieldSchema fieldSchemaV2 = fieldSchemaMapV2.get(fieldSchemaV1.getName());
            Assertions.assertEquals(fieldSchemaV2.getDescription(), fieldSchemaV1.getDescription());
            Assertions.assertEquals(fieldSchemaV2.getDataType().name(), fieldSchemaV1.getDataType().name());
            Assertions.assertEquals(fieldSchemaV2.getIsPrimaryKey(), fieldSchemaV1.isPrimaryKey());
            Assertions.assertEquals(fieldSchemaV2.getIsPartitionKey(), fieldSchemaV1.isPartitionKey());
            Assertions.assertEquals(fieldSchemaV2.getIsClusteringKey(), fieldSchemaV1.isClusteringKey());
            Assertions.assertEquals(fieldSchemaV2.getAutoID(), fieldSchemaV1.isAutoID());

            if (fieldSchemaV2.getDimension() != null) {
                Assertions.assertEquals(fieldSchemaV2.getDimension(), fieldSchemaV1.getDimension());
            }
            if(fieldSchemaV2.getDataType() == DataType.VarChar) {
                Assertions.assertEquals(fieldSchemaV2.getMaxLength(), fieldSchemaV1.getMaxLength());
            }

            if (fieldSchemaV2.getDataType() == DataType.Array) {
                Assertions.assertEquals(fieldSchemaV2.getMaxCapacity(), fieldSchemaV1.getMaxCapacity());
                Assertions.assertEquals(fieldSchemaV2.getElementType().name(), fieldSchemaV1.getElementType().name());
                if (fieldSchemaV2.getElementType() == DataType.VarChar) {
                    Assertions.assertEquals(fieldSchemaV2.getMaxLength(), fieldSchemaV1.getMaxLength());
                }
            }
        }
    }
}

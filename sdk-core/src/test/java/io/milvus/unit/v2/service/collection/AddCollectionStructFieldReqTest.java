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

package io.milvus.unit.v2.service.collection;

import io.milvus.exception.ParamException;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddCollectionStructFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AddCollectionStructFieldReqTest {

    @Test
    void builderBuildsWithAllFields() {
        CreateCollectionReq.FieldSchema subField = CreateCollectionReq.FieldSchema.builder()
                .name("content")
                .dataType(DataType.Text)
                .build();
        Map<String, String> typeParams = Collections.singletonMap("max_capacity", "10");
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .fieldName("chunks")
                .description("chunks field")
                .maxCapacity(20)
                .nullable(true)
                .structFields(Collections.singletonList(subField))
                .typeParams(typeParams)
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("chunks", request.getFieldName());
        assertEquals("chunks field", request.getDescription());
        assertEquals(20, request.getMaxCapacity());
        assertTrue(request.getNullable());
        assertEquals(Collections.singletonList(subField), request.getStructFields());
        assertEquals(typeParams, request.getTypeParams());
    }

    @Test
    void defaultsAreApplied() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder().build();
        assertEquals("", request.getCollectionName());
        assertEquals("", request.getDatabaseName());
        assertEquals("", request.getFieldName());
        assertEquals("", request.getDescription());
        assertTrue(request.getNullable());
        assertTrue(request.getStructFields().isEmpty());
        assertTrue(request.getTypeParams().isEmpty());
    }

    @Test
    void settersUpdateGetters() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setFieldName("f2");
        assertEquals("f2", request.getFieldName());

        request.setDescription("d");
        assertEquals("d", request.getDescription());

        request.setMaxCapacity(5);
        assertEquals(5, request.getMaxCapacity());

        request.setNullable(false);
        assertFalse(request.getNullable());

        request.setTypeParams(Collections.singletonMap("k", "v"));
        assertEquals(Collections.singletonMap("k", "v"), request.getTypeParams());
    }

    @Test
    void addStructFieldConvertsFieldReqToSchema() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .collectionName("coll")
                .fieldName("chunks")
                .addStructField(AddFieldReq.builder()
                        .fieldName("content")
                        .dataType(DataType.Text)
                        .build())
                .build();

        assertEquals(1, request.getStructFields().size());
        assertEquals("content", request.getStructFields().get(0).getName());
        assertEquals(DataType.Text, request.getStructFields().get(0).getDataType());
    }

    @Test
    void typeParamAddsSingleEntry() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .typeParam("a", "1")
                .typeParam("b", "2")
                .build();
        assertEquals("1", request.getTypeParams().get("a"));
        assertEquals("2", request.getTypeParams().get("b"));
    }

    @Test
    void toStructFieldSchemaConvertsAndForcesNullable() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .fieldName("chunks")
                .description("desc")
                .maxCapacity(10)
                .nullable(true)
                .structFields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("content")
                        .dataType(DataType.Text)
                        .build()))
                .build();

        CreateCollectionReq.StructFieldSchema schema = request.toStructFieldSchema();
        assertEquals("chunks", schema.getName());
        assertEquals("desc", schema.getDescription());
        assertEquals(10, schema.getMaxCapacity());
        assertTrue(schema.getNullable());
        assertEquals(DataType.Array, schema.getDataType());
        assertEquals(DataType.Struct, schema.getElementType());
    }

    @Test
    void toStructFieldSchemaRejectsNonNullable() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .fieldName("chunks")
                .nullable(false)
                .build();
        assertThrows(ParamException.class, request::toStructFieldSchema);
    }

    @Test
    void toStructFieldSchemaWithNullNullableSucceeds() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .fieldName("chunks")
                .maxCapacity(10)
                .nullable(null)
                .structFields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("content")
                        .dataType(DataType.Text)
                        .build()))
                .build();
        CreateCollectionReq.StructFieldSchema schema = request.toStructFieldSchema();
        assertNotNull(schema);
        assertTrue(schema.getNullable());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(AddCollectionStructFieldReq.builder());
    }

    @Test
    void toStringContainsFields() {
        AddCollectionStructFieldReq request = AddCollectionStructFieldReq.builder()
                .collectionName("coll")
                .build();
        assertTrue(request.toString().contains("coll"));
    }
}

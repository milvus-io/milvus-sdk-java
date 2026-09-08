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

import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddCollectionFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AddCollectionFieldReqTest {

    @Test
    void builderBuildsWithAllFieldsIncludingInherited() {
        CreateCollectionReq.FieldSchema subField = CreateCollectionReq.FieldSchema.builder()
                .name("content")
                .dataType(DataType.Text)
                .build();
        AddCollectionFieldReq request = AddCollectionFieldReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .fieldName("chunks")
                .description("struct field")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxLength(1024)
                .dimension(128)
                .maxCapacity(10)
                .isPrimaryKey(true)
                .isPartitionKey(false)
                .isClusteringKey(true)
                .autoID(false)
                .isNullable(true)
                .defaultValue("dv")
                .enableAnalyzer(true)
                .enableMatch(false)
                .typeParams(Collections.singletonMap("k", "v"))
                .multiAnalyzerParams(Collections.singletonMap("m", 1))
                .externalField("ext")
                .structFields(Collections.singletonList(subField))
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("chunks", request.getFieldName());
        assertEquals("struct field", request.getDescription());
        assertEquals(DataType.Array, request.getDataType());
        assertEquals(DataType.Struct, request.getElementType());
        assertEquals(1024, request.getMaxLength());
        assertEquals(128, request.getDimension());
        assertEquals(10, request.getMaxCapacity());
        assertTrue(request.getIsPrimaryKey());
        assertFalse(request.getIsPartitionKey());
        assertTrue(request.getIsClusteringKey());
        assertFalse(request.getAutoID());
        assertTrue(request.getIsNullable());
        assertEquals("dv", request.getDefaultValue());
        assertTrue(request.getEnableAnalyzer());
        assertFalse(request.getEnableMatch());
        assertEquals(Collections.singletonMap("k", "v"), request.getTypeParams());
        assertEquals(Collections.singletonMap("m", 1), request.getMultiAnalyzerParams());
        assertEquals("ext", request.getExternalField());
        assertEquals(Collections.singletonList(subField), request.getStructFields());
    }

    @Test
    void inheritedFieldsApplyDefaults() {
        AddCollectionFieldReq request = AddCollectionFieldReq.builder()
                .collectionName("coll")
                .fieldName("f")
                .build();

        assertEquals("", request.getDescription());
        assertEquals(65535, request.getMaxLength());
        assertFalse(request.getIsPrimaryKey());
        assertFalse(request.getIsPartitionKey());
        assertFalse(request.getIsClusteringKey());
        assertFalse(request.getAutoID());
        assertFalse(request.getIsNullable());
        assertNull(request.getDataType());
        assertNull(request.getDimension());
        assertEquals("", request.getExternalField());
        assertTrue(request.getStructFields().isEmpty());
    }

    @Test
    void settersUpdateGetters() {
        AddCollectionFieldReq request = AddCollectionFieldReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setFieldName("f2");
        assertEquals("f2", request.getFieldName());

        request.setDataType(DataType.FloatVector);
        assertEquals(DataType.FloatVector, request.getDataType());

        request.setDescription("d");
        assertEquals("d", request.getDescription());
    }

    @Test
    void addStructFieldAppendsSubField() {
        AddCollectionFieldReq request = AddCollectionFieldReq.builder()
                .collectionName("coll")
                .addStructField(io.milvus.v2.service.collection.request.AddFieldReq.builder()
                        .fieldName("content")
                        .dataType(DataType.Text)
                        .build())
                .build();

        assertEquals(1, request.getStructFields().size());
        assertEquals("content", request.getStructFields().get(0).getName());
        assertEquals(DataType.Text, request.getStructFields().get(0).getDataType());
    }

    @Test
    void toStringContainsFields() {
        AddCollectionFieldReq request = AddCollectionFieldReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }
}

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

package io.milvus.unit.v1.param;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.exception.ParamException;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.grpc.DataType;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CollectionSchemaParamTest {

    private FieldType pkField() {
        return FieldType.newBuilder()
                .withName("pk")
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .build();
    }

    private FieldType floatField() {
        return FieldType.newBuilder()
                .withName("vector")
                .withDataType(DataType.FloatVector)
                .withDimension(4)
                .build();
    }

    @Test
    void builderSetsAllFields() {
        FieldType fieldA = pkField();
        FieldType fieldB = floatField();

        CollectionSchemaParam param = CollectionSchemaParam.newBuilder()
                .withFieldTypes(Arrays.asList(fieldA, fieldB))
                .withEnableDynamicField(true)
                .build();

        assertEquals(Arrays.asList(fieldA, fieldB), param.getFieldTypes());
        assertTrue(param.isEnableDynamicField());
    }

    @Test
    void enableDynamicFieldDefaultsToFalse() {
        CollectionSchemaParam param = CollectionSchemaParam.newBuilder()
                .addFieldType(pkField())
                .build();

        assertFalse(param.isEnableDynamicField());
    }

    @Test
    void addFieldTypeAccumulates() {
        CollectionSchemaParam param = CollectionSchemaParam.newBuilder()
                .addFieldType(pkField())
                .addFieldType(floatField())
                .build();

        assertEquals(2, param.getFieldTypes().size());
    }

    @Test
    void emptyFieldTypesIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> CollectionSchemaParam.newBuilder().build());
    }

    @Test
    void nullFieldTypesIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CollectionSchemaParam.newBuilder().withFieldTypes(null));
    }

    @Test
    void nullAddFieldTypeIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CollectionSchemaParam.newBuilder().addFieldType(null));
    }

    @Test
    void nullFieldInsideListIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> CollectionSchemaParam.newBuilder()
                        .withFieldTypes(Collections.singletonList(null))
                        .build());
    }

    @Test
    void multiplePartitionKeyFieldsAreRejected() {
        FieldType partitionKey = FieldType.newBuilder()
                .withName("pk_col")
                .withDataType(DataType.VarChar)
                .withMaxLength(32)
                .withPartitionKey(true)
                .build();

        CollectionSchemaParam.Builder builder = CollectionSchemaParam.newBuilder()
                .addFieldType(partitionKey)
                .addFieldType(partitionKey);

        assertThrows(ParamException.class, builder::build);
    }
}

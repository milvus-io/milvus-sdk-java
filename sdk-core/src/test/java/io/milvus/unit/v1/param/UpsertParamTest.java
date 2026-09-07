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

import com.google.gson.JsonObject;
import io.milvus.exception.ParamException;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.UpsertParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class UpsertParamTest {

    private InsertParam.Field field(String name, Object... values) {
        return InsertParam.Field.builder()
                .name(name)
                .values(Arrays.asList(values))
                .build();
    }

    @Test
    void builderSetsAllFieldsWithColumns() {
        InsertParam.Field id = field("id", 1L, 2L);
        InsertParam.Field name = field("name", "a", "b");

        UpsertParam param = UpsertParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionName("part")
                .withFields(Arrays.asList(id, name))
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals("part", param.getPartitionName());
        assertEquals(Arrays.asList(id, name), param.getFields());
        assertNull(param.getRows());
        assertEquals(2, param.getRowCount());
    }

    @Test
    void builderSetsAllFieldsWithRows() {
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.addProperty("name", "a");

        UpsertParam param = UpsertParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withRows(Collections.singletonList(row))
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals(Collections.singletonList(row), param.getRows());
        assertNull(param.getFields());
        assertEquals(1, param.getRowCount());
    }

    @Test
    void defaults() {
        UpsertParam param = UpsertParam.newBuilder()
                .withCollectionName("coll")
                .withFields(Collections.singletonList(field("id", 1L)))
                .build();

        assertNull(param.getDatabaseName());
        assertEquals("", param.getPartitionName());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> UpsertParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder()
                        .withCollectionName("")
                        .withFields(Collections.singletonList(field("id", 1L)))
                        .build());
    }

    @Test
    void missingFieldsAndRowsIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder().withCollectionName("coll").build());
    }

    @Test
    void bothFieldsAndRowsIsRejectedByBuild() {
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);

        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder()
                        .withCollectionName("coll")
                        .withFields(Collections.singletonList(field("id", 1L)))
                        .withRows(Collections.singletonList(row))
                        .build());
    }

    @Test
    void emptyFieldNameIsRejectedByBuild() {
        InsertParam.Field emptyName = InsertParam.Field.builder().name("").values(Arrays.asList(1L)).build();

        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder()
                        .withCollectionName("coll")
                        .withFields(Collections.singletonList(emptyName))
                        .build());
    }

    @Test
    void emptyFieldValuesIsRejectedByBuild() {
        InsertParam.Field emptyValues = InsertParam.Field.builder().name("id").values(Collections.emptyList()).build();

        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder()
                        .withCollectionName("coll")
                        .withFields(Collections.singletonList(emptyValues))
                        .build());
    }

    @Test
    void mismatchedFieldRowCountIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder()
                        .withCollectionName("coll")
                        .withFields(Arrays.asList(field("id", 1L, 2L), field("name", "a")))
                        .build());
    }

    @Test
    void nullFieldNameIsRejectedByBuild() {
        InsertParam.Field nullName = InsertParam.Field.builder().name(null).values(Arrays.asList(1L)).build();

        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder()
                        .withCollectionName("coll")
                        .withFields(Collections.singletonList(nullName))
                        .build());
    }

    @Test
    void nullFieldsIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> UpsertParam.newBuilder().withCollectionName("coll").withFields(null));
    }

    @Test
    void nullRowsIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> UpsertParam.newBuilder().withCollectionName("coll").withRows(null));
    }

    @Test
    void nullPartitionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> UpsertParam.newBuilder().withCollectionName("coll").withPartitionName(null));
    }

    @Test
    void nullRowInRowsIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> UpsertParam.newBuilder()
                        .withCollectionName("coll")
                        .withRows(Collections.singletonList(null))
                        .build());
    }
}

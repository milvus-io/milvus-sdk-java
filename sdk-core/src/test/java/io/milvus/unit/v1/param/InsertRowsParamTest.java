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
import io.milvus.param.highlevel.dml.InsertRowsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class InsertRowsParamTest {
    @Test
    void buildAndGet() {
        List<JsonObject> rows = new ArrayList<>();
        JsonObject row = new JsonObject();
        row.addProperty("id", 1L);
        row.addProperty("vector", "[]");
        rows.add(row);

        InsertRowsParam param = InsertRowsParam.newBuilder()
                .withCollectionName("coll1")
                .withRows(rows)
                .build();

        assertEquals("coll1", param.getInsertParam().getCollectionName());
        assertEquals(1, param.getInsertParam().getRowCount());
        assertNotNull(param.toString());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        List<JsonObject> rows = new ArrayList<>();
        rows.add(new JsonObject());
        assertThrows(ParamException.class, () -> InsertRowsParam.newBuilder()
                .withRows(rows).build());
    }

    @Test
    void buildFailsWhenRowsEmpty() {
        assertThrows(ParamException.class, () -> InsertRowsParam.newBuilder()
                .withCollectionName("coll1")
                .withRows(new ArrayList<>())
                .build());
        assertThrows(ParamException.class, () -> InsertRowsParam.newBuilder()
                .withCollectionName("coll1")
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                InsertRowsParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                InsertRowsParam.newBuilder().withRows(null));
    }
}

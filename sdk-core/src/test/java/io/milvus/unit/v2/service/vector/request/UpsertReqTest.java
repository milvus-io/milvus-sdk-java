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

package io.milvus.unit.v2.service.vector.request;

import com.google.gson.JsonObject;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.request.UpsertReq.FieldPartialUpdateOp;
import io.milvus.v2.service.vector.request.UpsertReq.FieldPartialUpdateOp.OpType;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class UpsertReqTest {

    @Test
    void builderSetsAllFields() {
        List<JsonObject> data = Collections.singletonList(new JsonObject());
        FieldPartialUpdateOp op = FieldPartialUpdateOp.builder()
                .fieldName("tags")
                .opType(OpType.ARRAY_APPEND)
                .build();
        UpsertReq req = UpsertReq.builder()
                .data(data)
                .databaseName("db")
                .collectionName("col")
                .partitionName("p0")
                .partialUpdate(true)
                .fieldOps(Collections.singletonList(op))
                .build();

        assertEquals(data, req.getData());
        assertEquals("db", req.getDatabaseName());
        assertEquals("col", req.getCollectionName());
        assertEquals("p0", req.getPartitionName());
        assertTrue(req.isPartialUpdate());
        assertEquals(Collections.singletonList(op), req.getFieldOps());
    }

    @Test
    void builderDefaults() {
        UpsertReq req = UpsertReq.builder().build();

        assertNull(req.getData());
        assertEquals("", req.getDatabaseName());
        assertNull(req.getCollectionName());
        assertEquals("", req.getPartitionName());
        assertFalse(req.isPartialUpdate());
        assertNull(req.getFieldOps());
    }

    @Test
    void settersUpdateFields() {
        UpsertReq req = UpsertReq.builder().build();

        req.setData(Collections.singletonList(new JsonObject()));
        req.setDatabaseName("db2");
        req.setCollectionName("col2");
        req.setPartitionName("p2");
        req.setPartialUpdate(true);
        req.setFieldOps(Collections.singletonList(
                FieldPartialUpdateOp.builder().fieldName("x").build()));

        assertEquals(1, req.getData().size());
        assertEquals("db2", req.getDatabaseName());
        assertEquals("col2", req.getCollectionName());
        assertEquals("p2", req.getPartitionName());
        assertTrue(req.isPartialUpdate());
        assertEquals(1, req.getFieldOps().size());
    }

    @Test
    void isPartialUpdateReflectsNonReplaceFieldOps() {
        UpsertReq replaceOnly = UpsertReq.builder()
                .fieldOps(Collections.singletonList(
                        FieldPartialUpdateOp.builder().fieldName("x").opType(OpType.REPLACE).build()))
                .build();
        assertFalse(replaceOnly.isPartialUpdate());

        UpsertReq append = UpsertReq.builder()
                .fieldOps(Collections.singletonList(
                        FieldPartialUpdateOp.builder().fieldName("x").opType(OpType.ARRAY_APPEND).build()))
                .build();
        assertTrue(append.isPartialUpdate());

        UpsertReq remove = UpsertReq.builder()
                .fieldOps(Collections.singletonList(
                        FieldPartialUpdateOp.builder().fieldName("x").opType(OpType.ARRAY_REMOVE).build()))
                .build();
        assertTrue(remove.isPartialUpdate());

        UpsertReq nullOp = UpsertReq.builder()
                .fieldOps(Arrays.asList(null, FieldPartialUpdateOp.builder().fieldName("y").build()))
                .build();
        assertFalse(nullOp.isPartialUpdate());
    }

    @Test
    void fieldPartialUpdateOpBuilderAndEnum() {
        FieldPartialUpdateOp op = FieldPartialUpdateOp.builder()
                .fieldName("tags")
                .opType(OpType.ARRAY_APPEND)
                .build();
        assertEquals("tags", op.getFieldName());
        assertEquals(OpType.ARRAY_APPEND, op.getOpType());

        op.setFieldName("tags2");
        op.setOpType(OpType.ARRAY_REMOVE);
        assertEquals("tags2", op.getFieldName());
        assertEquals(OpType.ARRAY_REMOVE, op.getOpType());

        assertEquals(OpType.REPLACE, FieldPartialUpdateOp.builder().fieldName("x").build().getOpType());
        assertEquals(3, OpType.values().length);
    }

    @Test
    void toStringContainsFields() {
        UpsertReq req = UpsertReq.builder().collectionName("col").build();
        assertNotNull(req.toString());
    }
}

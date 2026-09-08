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

import io.milvus.exception.ParamException;
import io.milvus.param.control.GetFlushStateParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class GetFlushStateParamTest {
    @Test
    void buildAndGet() {
        GetFlushStateParam param = GetFlushStateParam.newBuilder()
                .withDatabaseName("db1")
                .withCollectionName("coll1")
                .withSegmentIDs(Arrays.asList(1L, 2L))
                .withFlushTs(123L)
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("coll1", param.getCollectionName());
        assertEquals(Arrays.asList(1L, 2L), param.getSegmentIDs());
        assertEquals(Long.valueOf(123L), param.getFlushTs());
    }

    @Test
    void defaults() {
        GetFlushStateParam param = GetFlushStateParam.newBuilder()
                .withCollectionName("coll1")
                .build();

        assertNull(param.getDatabaseName());
        assertEquals(Long.valueOf(0L), param.getFlushTs());
        assertEquals(0, param.getSegmentIDs().size());
    }

    @Test
    void addSegmentID() {
        GetFlushStateParam param = GetFlushStateParam.newBuilder()
                .withCollectionName("coll1")
                .addSegmentID(5L)
                .build();

        assertEquals(Arrays.asList(5L), param.getSegmentIDs());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> GetFlushStateParam.newBuilder().build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                GetFlushStateParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetFlushStateParam.newBuilder().withSegmentIDs(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetFlushStateParam.newBuilder().addSegmentID(null));
        assertThrows(IllegalArgumentException.class, () ->
                GetFlushStateParam.newBuilder().withFlushTs(null));
    }
}

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
import io.milvus.param.control.LoadBalanceParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class LoadBalanceParamTest {
    @Test
    void buildAndGet() {
        LoadBalanceParam param = LoadBalanceParam.newBuilder()
                .withDatabaseName("db1")
                .withCollectionName("coll1")
                .withSourceNodeID(1L)
                .withDestinationNodeID(Arrays.asList(2L, 3L))
                .withSegmentIDs(Arrays.asList(10L, 11L))
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("coll1", param.getCollectionName());
        assertEquals(1L, param.getSrcNodeID());
        assertEquals(Arrays.asList(2L, 3L), param.getDestNodeIDs());
        assertEquals(Arrays.asList(10L, 11L), param.getSegmentIDs());
    }

    @Test
    void addMethods() {
        LoadBalanceParam param = LoadBalanceParam.newBuilder()
                .withSourceNodeID(1L)
                .addDestinationNodeID(2L)
                .addDestinationNodeID(2L)
                .addSegmentID(10L)
                .build();

        assertEquals(Arrays.asList(2L), param.getDestNodeIDs());
        assertEquals(Arrays.asList(10L), param.getSegmentIDs());
    }

    @Test
    void buildFailsWhenSegmentIdsEmpty() {
        assertThrows(ParamException.class, () -> LoadBalanceParam.newBuilder()
                .withDestinationNodeID(Arrays.asList(2L))
                .build());
    }

    @Test
    void buildFailsWhenDestNodeIdsEmpty() {
        assertThrows(ParamException.class, () -> LoadBalanceParam.newBuilder()
                .withSegmentIDs(Arrays.asList(10L))
                .build());
    }

    @Test
    void optionalFields() {
        LoadBalanceParam param = LoadBalanceParam.newBuilder()
                .addDestinationNodeID(2L)
                .addSegmentID(10L)
                .build();

        assertNull(param.getDatabaseName());
        assertNull(param.getCollectionName());
        assertNull(param.getSrcNodeID());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                LoadBalanceParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadBalanceParam.newBuilder().withSourceNodeID(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadBalanceParam.newBuilder().addDestinationNodeID(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadBalanceParam.newBuilder().withDestinationNodeID(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadBalanceParam.newBuilder().addSegmentID(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadBalanceParam.newBuilder().withSegmentIDs(null));
    }
}

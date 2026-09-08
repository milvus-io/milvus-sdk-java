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
import io.milvus.param.Constant;
import io.milvus.param.partition.LoadPartitionsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class LoadPartitionsParamTest {
    @Test
    void buildAndGet() {
        LoadPartitionsParam param = LoadPartitionsParam.newBuilder()
                .withDatabaseName("db1")
                .withCollectionName("coll1")
                .withPartitionNames(Arrays.asList("p1", "p2"))
                .withSyncLoad(Boolean.FALSE)
                .withSyncLoadWaitingInterval(100L)
                .withSyncLoadWaitingTimeout(30L)
                .withReplicaNumber(2)
                .withRefresh(Boolean.TRUE)
                .withResourceGroups(Arrays.asList("rg1", "rg2"))
                .withLoadFields(Arrays.asList("field1", "field2"))
                .withSkipLoadDynamicField(Boolean.TRUE)
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("coll1", param.getCollectionName());
        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
        assertFalse(param.isSyncLoad());
        assertEquals(100L, param.getSyncLoadWaitingInterval());
        assertEquals(30L, param.getSyncLoadWaitingTimeout());
        assertEquals(2, param.getReplicaNumber());
        assertTrue(param.isRefresh());
        assertEquals(Arrays.asList("rg1", "rg2"), param.getResourceGroups());
        assertEquals(Arrays.asList("field1", "field2"), param.getLoadFields());
        assertTrue(param.isSkipLoadDynamicField());
    }

    @Test
    void defaults() {
        LoadPartitionsParam param = LoadPartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .build();

        assertTrue(param.isSyncLoad());
        assertEquals(500L, param.getSyncLoadWaitingInterval());
        assertEquals(60L, param.getSyncLoadWaitingTimeout());
        assertEquals(0, param.getReplicaNumber());
        assertFalse(param.isRefresh());
        assertTrue(param.getResourceGroups().isEmpty());
        assertTrue(param.getLoadFields().isEmpty());
        assertFalse(param.isSkipLoadDynamicField());
    }

    @Test
    void addPartitionNameDeduplicates() {
        LoadPartitionsParam param = LoadPartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .addPartitionName("p1")
                .build();

        assertEquals(Collections.singletonList("p1"), param.getPartitionNames());
    }

    @Test
    void buildFailsWhenCollectionNameMissing() {
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .addPartitionName("p1").build());
    }

    @Test
    void buildFailsWhenPartitionNamesEmpty() {
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("coll1").build());
    }

    @Test
    void buildFailsWhenIntervalInvalid() {
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .withSyncLoadWaitingInterval(0L)
                .build());
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .withSyncLoadWaitingInterval(Constant.MAX_WAITING_LOADING_INTERVAL + 1)
                .build());
    }

    @Test
    void buildFailsWhenTimeoutInvalid() {
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .withSyncLoadWaitingTimeout(0L)
                .build());
        assertThrows(ParamException.class, () -> LoadPartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .withSyncLoadWaitingTimeout(Constant.MAX_WAITING_LOADING_TIMEOUT + 1)
                .build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withPartitionNames(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().addPartitionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withSyncLoad(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withSyncLoadWaitingInterval(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withSyncLoadWaitingTimeout(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withReplicaNumber(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withRefresh(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withResourceGroups(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withLoadFields(null));
        assertThrows(IllegalArgumentException.class, () ->
                LoadPartitionsParam.newBuilder().withSkipLoadDynamicField(null));
    }
}

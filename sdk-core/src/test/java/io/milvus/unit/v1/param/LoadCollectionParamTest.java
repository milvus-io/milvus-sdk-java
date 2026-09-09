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
import io.milvus.param.collection.LoadCollectionParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class LoadCollectionParamTest {

    @Test
    void builderSetsAllFields() {
        List<String> resourceGroups = Arrays.asList("rg1", "rg2");
        List<String> loadFields = Arrays.asList("field_a", "field_b");

        LoadCollectionParam param = LoadCollectionParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withSyncLoad(true)
                .withSyncLoadWaitingInterval(100L)
                .withSyncLoadWaitingTimeout(30L)
                .withReplicaNumber(2)
                .withRefresh(true)
                .withResourceGroups(resourceGroups)
                .withLoadFields(loadFields)
                .withSkipLoadDynamicField(true)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertTrue(param.isSyncLoad());
        assertEquals(100L, param.getSyncLoadWaitingInterval());
        assertEquals(30L, param.getSyncLoadWaitingTimeout());
        assertEquals(2, param.getReplicaNumber());
        assertTrue(param.isRefresh());
        assertEquals(resourceGroups, param.getResourceGroups());
        assertEquals(loadFields, param.getLoadFields());
        assertTrue(param.isSkipLoadDynamicField());
    }

    @Test
    void builderDefaults() {
        LoadCollectionParam param = LoadCollectionParam.newBuilder()
                .withCollectionName("coll")
                .build();

        assertNull(param.getDatabaseName());
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
    void loadFieldsDeDuplicates() {
        LoadCollectionParam param = LoadCollectionParam.newBuilder()
                .withCollectionName("coll")
                .withLoadFields(Arrays.asList("a", "a", "b"))
                .withLoadFields(Arrays.asList("b", "c"))
                .build();

        assertEquals(Arrays.asList("a", "b", "c"), param.getLoadFields());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LoadCollectionParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> LoadCollectionParam.newBuilder().withCollectionName("").build());
    }

    @Test
    void missingCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> LoadCollectionParam.newBuilder().build());
    }

    @Test
    void nullSyncLoadIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LoadCollectionParam.newBuilder().withCollectionName("coll").withSyncLoad(null));
    }

    @Test
    void zeroSyncLoadWaitingIntervalIsRejected() {
        assertThrows(ParamException.class,
                () -> LoadCollectionParam.newBuilder()
                        .withCollectionName("coll")
                        .withSyncLoad(true)
                        .withSyncLoadWaitingInterval(0L)
                        .build());
    }

    @Test
    void tooLargeSyncLoadWaitingIntervalIsRejected() {
        assertThrows(ParamException.class,
                () -> LoadCollectionParam.newBuilder()
                        .withCollectionName("coll")
                        .withSyncLoad(true)
                        .withSyncLoadWaitingInterval(5000L)
                        .build());
    }

    @Test
    void zeroSyncLoadWaitingTimeoutIsRejected() {
        assertThrows(ParamException.class,
                () -> LoadCollectionParam.newBuilder()
                        .withCollectionName("coll")
                        .withSyncLoad(true)
                        .withSyncLoadWaitingTimeout(0L)
                        .build());
    }

    @Test
    void tooLargeSyncLoadWaitingTimeoutIsRejected() {
        assertThrows(ParamException.class,
                () -> LoadCollectionParam.newBuilder()
                        .withCollectionName("coll")
                        .withSyncLoad(true)
                        .withSyncLoadWaitingTimeout(1000L)
                        .build());
    }

    @Test
    void invalidSyncTimingIgnoredWhenSyncLoadIsFalse() {
        LoadCollectionParam param = LoadCollectionParam.newBuilder()
                .withCollectionName("coll")
                .withSyncLoad(false)
                .withSyncLoadWaitingInterval(0L)
                .withSyncLoadWaitingTimeout(-1L)
                .build();

        assertFalse(param.isSyncLoad());
        assertEquals(0L, param.getSyncLoadWaitingInterval());
        assertEquals(-1L, param.getSyncLoadWaitingTimeout());
    }

    @Test
    void negativeReplicaNumberIsRejected() {
        assertThrows(ParamException.class,
                () -> LoadCollectionParam.newBuilder()
                        .withCollectionName("coll")
                        .withReplicaNumber(-1)
                        .build());
    }

    @Test
    void nullReplicaNumberIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LoadCollectionParam.newBuilder().withCollectionName("coll").withReplicaNumber(null));
    }

    @Test
    void nullResourceGroupsIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LoadCollectionParam.newBuilder().withCollectionName("coll").withResourceGroups(null));
    }

    @Test
    void nullLoadFieldsIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LoadCollectionParam.newBuilder().withCollectionName("coll").withLoadFields(null));
    }

    @Test
    void nullSkipLoadDynamicFieldIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> LoadCollectionParam.newBuilder().withCollectionName("coll").withSkipLoadDynamicField(null));
    }

    @Test
    void emptyResourceGroupListIsAllowed() {
        LoadCollectionParam param = LoadCollectionParam.newBuilder()
                .withCollectionName("coll")
                .withResourceGroups(Collections.emptyList())
                .build();

        assertTrue(param.getResourceGroups().isEmpty());
    }
}

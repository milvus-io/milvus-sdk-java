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
import io.milvus.param.collection.FlushParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class FlushParamTest {

    @Test
    void builderSetsAllFields() {
        List<String> names = Arrays.asList("coll_a", "coll_b");

        FlushParam param = FlushParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionNames(names)
                .withSyncFlush(true)
                .withSyncFlushWaitingInterval(100L)
                .withSyncFlushWaitingTimeout(30L)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals(names, param.getCollectionNames());
        assertEquals(Boolean.TRUE, param.getSyncFlush());
        assertEquals(100L, param.getSyncFlushWaitingInterval());
        assertEquals(30L, param.getSyncFlushWaitingTimeout());
    }

    @Test
    void builderDefaults() {
        FlushParam param = FlushParam.newBuilder()
                .addCollectionName("coll")
                .build();

        assertNull(param.getDatabaseName());
        assertEquals(Boolean.TRUE, param.getSyncFlush());
        assertEquals(500L, param.getSyncFlushWaitingInterval());
        assertEquals(60L, param.getSyncFlushWaitingTimeout());
    }

    @Test
    void addCollectionNameAccumulates() {
        FlushParam param = FlushParam.newBuilder()
                .addCollectionName("a")
                .addCollectionName("b")
                .build();

        assertEquals(Arrays.asList("a", "b"), param.getCollectionNames());
    }

    @Test
    void withCollectionNamesAccumulates() {
        FlushParam param = FlushParam.newBuilder()
                .withCollectionNames(Arrays.asList("a", "b"))
                .withCollectionNames(Arrays.asList("c"))
                .build();

        assertEquals(Arrays.asList("a", "b", "c"), param.getCollectionNames());
    }

    @Test
    void nullCollectionNamesIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> FlushParam.newBuilder().withCollectionNames(null));
    }

    @Test
    void nullAddCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> FlushParam.newBuilder().addCollectionName(null));
    }

    @Test
    void emptyCollectionNamesIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> FlushParam.newBuilder().build());
    }

    @Test
    void emptyNameInsideListIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> FlushParam.newBuilder().withCollectionNames(Collections.singletonList("")).build());
    }

    @Test
    void zeroSyncFlushWaitingIntervalIsRejected() {
        assertThrows(ParamException.class,
                () -> FlushParam.newBuilder()
                        .addCollectionName("coll")
                        .withSyncFlush(true)
                        .withSyncFlushWaitingInterval(0L)
                        .build());
    }

    @Test
    void tooLargeSyncFlushWaitingIntervalIsRejected() {
        assertThrows(ParamException.class,
                () -> FlushParam.newBuilder()
                        .addCollectionName("coll")
                        .withSyncFlush(true)
                        .withSyncFlushWaitingInterval(5000L)
                        .build());
    }

    @Test
    void zeroSyncFlushWaitingTimeoutIsRejected() {
        assertThrows(ParamException.class,
                () -> FlushParam.newBuilder()
                        .addCollectionName("coll")
                        .withSyncFlush(true)
                        .withSyncFlushWaitingTimeout(0L)
                        .build());
    }

    @Test
    void tooLargeSyncFlushWaitingTimeoutIsRejected() {
        assertThrows(ParamException.class,
                () -> FlushParam.newBuilder()
                        .addCollectionName("coll")
                        .withSyncFlush(true)
                        .withSyncFlushWaitingTimeout(1000L)
                        .build());
    }

    @Test
    void invalidSyncTimingIgnoredWhenSyncFlushIsFalse() {
        FlushParam param = FlushParam.newBuilder()
                .addCollectionName("coll")
                .withSyncFlush(false)
                .withSyncFlushWaitingInterval(0L)
                .withSyncFlushWaitingTimeout(-1L)
                .build();

        assertEquals(Boolean.FALSE, param.getSyncFlush());
        assertEquals(0L, param.getSyncFlushWaitingInterval());
        assertEquals(-1L, param.getSyncFlushWaitingTimeout());
    }

    @Test
    void nullSyncFlushIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> FlushParam.newBuilder().addCollectionName("coll").withSyncFlush(null));
    }

    @Test
    void emptySyncFlushWaitingIntervalIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> FlushParam.newBuilder().addCollectionName("coll").withSyncFlushWaitingInterval(null));
    }

    @Test
    void emptySyncFlushWaitingTimeoutIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> FlushParam.newBuilder().addCollectionName("coll").withSyncFlushWaitingTimeout(null));
    }
}

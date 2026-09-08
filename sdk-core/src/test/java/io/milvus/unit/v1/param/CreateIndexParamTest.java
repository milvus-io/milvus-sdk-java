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
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.index.CreateIndexParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CreateIndexParamTest {

    @Test
    void builderSetsAllFields() {
        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withFieldName("vector")
                .withIndexType(IndexType.HNSW)
                .withIndexName("my_idx")
                .withMetricType(MetricType.L2)
                .withExtraParam("{\"M\":16}")
                .withSyncMode(true)
                .withSyncWaitingInterval(100L)
                .withSyncWaitingTimeout(60L)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals("vector", param.getFieldName());
        assertEquals(IndexType.HNSW, param.getIndexType());
        assertEquals("my_idx", param.getIndexName());
        assertTrue(param.isSyncMode());
        assertEquals(100L, param.getSyncWaitingInterval());
        assertEquals(60L, param.getSyncWaitingTimeout());

        assertEquals(IndexType.HNSW.getName(), param.getExtraParam().get(Constant.INDEX_TYPE));
        assertEquals(MetricType.L2.name(), param.getExtraParam().get(Constant.METRIC_TYPE));
        assertEquals("{\"M\":16}", param.getExtraParam().get(Constant.PARAMS));
    }

    @Test
    void builderDefaults() {
        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withCollectionName("coll")
                .withFieldName("vector")
                .build();

        assertNull(param.getDatabaseName());
        assertEquals(IndexType.None, param.getIndexType());
        assertEquals(Constant.DEFAULT_INDEX_NAME, param.getIndexName());
        assertTrue(param.isSyncMode());
        assertEquals(500L, param.getSyncWaitingInterval());
        assertEquals(600L, param.getSyncWaitingTimeout());
        assertTrue(param.getExtraParam().isEmpty());
    }

    @Test
    void extraParamOmitsNoneTypeEntries() {
        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withCollectionName("coll")
                .withFieldName("vector")
                .build();

        assertFalse(param.getExtraParam().containsKey(Constant.INDEX_TYPE));
        assertFalse(param.getExtraParam().containsKey(Constant.METRIC_TYPE));
    }

    @Test
    void blankIndexNameFallsBackToDefault() {
        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withCollectionName("coll")
                .withFieldName("vector")
                .withIndexName("  ")
                .build();

        assertEquals(Constant.DEFAULT_INDEX_NAME, param.getIndexName());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> CreateIndexParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> CreateIndexParam.newBuilder()
                        .withCollectionName("")
                        .withFieldName("vector")
                        .build());
    }

    @Test
    void emptyFieldNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> CreateIndexParam.newBuilder()
                        .withCollectionName("coll")
                        .withFieldName("")
                        .build());
    }

    @Test
    void missingFieldNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> CreateIndexParam.newBuilder().withCollectionName("coll").build());
    }

    @Test
    void zeroSyncWaitingIntervalIsRejected() {
        assertThrows(ParamException.class,
                () -> CreateIndexParam.newBuilder()
                        .withCollectionName("coll")
                        .withFieldName("vector")
                        .withSyncWaitingInterval(0L)
                        .build());
    }

    @Test
    void tooLargeSyncWaitingIntervalIsRejected() {
        assertThrows(ParamException.class,
                () -> CreateIndexParam.newBuilder()
                        .withCollectionName("coll")
                        .withFieldName("vector")
                        .withSyncWaitingInterval(5000L)
                        .build());
    }

    @Test
    void zeroSyncWaitingTimeoutIsRejected() {
        assertThrows(ParamException.class,
                () -> CreateIndexParam.newBuilder()
                        .withCollectionName("coll")
                        .withFieldName("vector")
                        .withSyncWaitingTimeout(0L)
                        .build());
    }

    @Test
    void invalidSyncTimingIgnoredWhenSyncModeIsFalse() {
        CreateIndexParam param = CreateIndexParam.newBuilder()
                .withCollectionName("coll")
                .withFieldName("vector")
                .withSyncMode(false)
                .withSyncWaitingInterval(0L)
                .withSyncWaitingTimeout(0L)
                .build();

        assertFalse(param.isSyncMode());
    }

    @Test
    void nullParamValuesAreRejected() {
        CreateIndexParam.Builder builder = CreateIndexParam.newBuilder()
                .withCollectionName("coll")
                .withFieldName("vector");

        assertThrows(IllegalArgumentException.class, () -> builder.withFieldName(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withIndexType(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withIndexName(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withMetricType(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withExtraParam(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withSyncMode(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withSyncWaitingInterval(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withSyncWaitingTimeout(null));
    }
}

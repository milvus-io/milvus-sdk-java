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
import io.milvus.param.Constant;
import io.milvus.param.dml.QueryIteratorParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class QueryIteratorParamTest {

    @Test
    void builderSetsAllFields() {
        QueryIteratorParam param = QueryIteratorParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionNames(Arrays.asList("p1", "p2"))
                .withOutFields(Arrays.asList("a", "b"))
                .withExpr("id > 1")
                .withOffset(5L)
                .withLimit(20L)
                .withBatchSize(500L)
                .withIgnoreGrowing(true)
                .withReduceStopForBest(false)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
        assertEquals(Arrays.asList("a", "b"), param.getOutFields());
        assertEquals("id > 1", param.getExpr());
        assertEquals(5L, param.getOffset());
        assertEquals(20L, param.getLimit());
        assertEquals(500L, param.getBatchSize());
        assertTrue(param.isIgnoreGrowing());
        assertFalse(param.isReduceStopForBest());
        assertEquals(ConsistencyLevelEnum.EVENTUALLY, param.getConsistencyLevel());
    }

    @Test
    void builderDefaults() {
        QueryIteratorParam param = QueryIteratorParam.newBuilder()
                .withCollectionName("coll")
                .build();

        assertNull(param.getDatabaseName());
        assertTrue(param.getPartitionNames().isEmpty());
        assertTrue(param.getOutFields().isEmpty());
        assertEquals("", param.getExpr());
        assertEquals(0L, param.getTravelTimestamp());
        assertEquals(1L, param.getGuaranteeTimestamp());
        assertEquals(5000L, param.getGracefulTime());
        assertNull(param.getConsistencyLevel());
        assertEquals(0L, param.getOffset());
        assertEquals(Constant.UNLIMITED, param.getLimit());
        assertEquals(1000L, param.getBatchSize());
        assertFalse(param.isIgnoreGrowing());
        assertTrue(param.isReduceStopForBest());
    }

    @Test
    void addPartitionNameAndOutFieldAccumulate() {
        QueryIteratorParam param = QueryIteratorParam.newBuilder()
                .withCollectionName("coll")
                .addPartitionName("p1")
                .addPartitionName("p1")
                .addPartitionName("p2")
                .addOutField("f1")
                .addOutField("f1")
                .addOutField("f2")
                .build();

        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
        assertEquals(Arrays.asList("f1", "f2"), param.getOutFields());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryIteratorParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> QueryIteratorParam.newBuilder().withCollectionName("").build());
    }

    @Test
    void missingCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> QueryIteratorParam.newBuilder().build());
    }

    @Test
    void nullExprIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryIteratorParam.newBuilder().withCollectionName("coll").withExpr(null));
    }

    @Test
    void emptyExprIsAllowed() {
        QueryIteratorParam param = QueryIteratorParam.newBuilder()
                .withCollectionName("coll")
                .withExpr("")
                .build();

        assertEquals("", param.getExpr());
    }

    @Test
    void negativeLimitIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> QueryIteratorParam.newBuilder()
                        .withCollectionName("coll")
                        .withLimit(-2L)
                        .build());
    }

    @Test
    void unlimitedLimitIsAllowed() {
        QueryIteratorParam param = QueryIteratorParam.newBuilder()
                .withCollectionName("coll")
                .withLimit((long) Constant.UNLIMITED)
                .build();

        assertEquals(Constant.UNLIMITED, param.getLimit());
    }

    @Test
    void negativeBatchSizeIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> QueryIteratorParam.newBuilder()
                        .withCollectionName("coll")
                        .withBatchSize(-1L)
                        .build());
    }

    @Test
    void tooLargeBatchSizeIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> QueryIteratorParam.newBuilder()
                        .withCollectionName("coll")
                        .withBatchSize((long) Constant.MAX_BATCH_SIZE + 1)
                        .build());
    }

    @Test
    void nullParamValuesAreRejected() {
        QueryIteratorParam.Builder builder = QueryIteratorParam.newBuilder().withCollectionName("coll");
        assertThrows(IllegalArgumentException.class, () -> builder.withPartitionNames(null));
        assertThrows(IllegalArgumentException.class, () -> builder.addPartitionName(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withOutFields(null));
        assertThrows(IllegalArgumentException.class, () -> builder.addOutField(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withOffset(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withLimit(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withIgnoreGrowing(null));
        assertThrows(IllegalArgumentException.class, () -> builder.withReduceStopForBest(null));
    }
}

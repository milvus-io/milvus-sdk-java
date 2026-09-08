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
import io.milvus.param.dml.QueryParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class QueryParamTest {

    @Test
    void builderSetsAllFields() {
        QueryParam param = QueryParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionNames(Arrays.asList("p1", "p2"))
                .withOutFields(Arrays.asList("a", "b"))
                .withExpr("id > 1")
                .withOffset(5L)
                .withLimit(20L)
                .withIgnoreGrowing(true)
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
        assertEquals(Arrays.asList("a", "b"), param.getOutFields());
        assertEquals("id > 1", param.getExpr());
        assertEquals(5L, param.getOffset());
        assertEquals(20L, param.getLimit());
        assertTrue(param.isIgnoreGrowing());
        assertEquals(ConsistencyLevelEnum.STRONG, param.getConsistencyLevel());
    }

    @Test
    void builderDefaults() {
        QueryParam param = QueryParam.newBuilder()
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
        assertEquals(0L, param.getLimit());
        assertFalse(param.isIgnoreGrowing());
    }

    @Test
    void setDatabaseNameUpdatesDatabaseName() {
        QueryParam param = QueryParam.newBuilder().withCollectionName("coll").build();
        param.setDatabaseName("updated");

        assertEquals("updated", param.getDatabaseName());
    }

    @Test
    void addPartitionNameAndOutFieldAccumulate() {
        QueryParam param = QueryParam.newBuilder()
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
                () -> QueryParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> QueryParam.newBuilder().withCollectionName("").build());
    }

    @Test
    void missingCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> QueryParam.newBuilder().build());
    }

    @Test
    void nullExprIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").withExpr(null));
    }

    @Test
    void emptyExprIsAllowed() {
        QueryParam param = QueryParam.newBuilder()
                .withCollectionName("coll")
                .withExpr("")
                .build();

        assertEquals("", param.getExpr());
    }

    @Test
    void nullOffsetIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").withOffset(null));
    }

    @Test
    void nullLimitIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").withLimit(null));
    }

    @Test
    void nullIgnoreGrowingIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").withIgnoreGrowing(null));
    }

    @Test
    void nullPartitionNamesIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").withPartitionNames(null));
    }

    @Test
    void nullOutFieldsIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").withOutFields(null));
    }

    @Test
    void nullAddPartitionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").addPartitionName(null));
    }

    @Test
    void nullAddOutFieldIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> QueryParam.newBuilder().withCollectionName("coll").addOutField(null));
    }
}

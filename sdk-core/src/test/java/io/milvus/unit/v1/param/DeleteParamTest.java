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
import io.milvus.param.dml.DeleteParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class DeleteParamTest {

    @Test
    void builderSetsAllFields() {
        DeleteParam param = DeleteParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionName("part")
                .withExpr("id in [1,2,3]")
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals("part", param.getPartitionName());
        assertEquals("id in [1,2,3]", param.getExpr());
    }

    @Test
    void partitionNameDefaultsToEmpty() {
        DeleteParam param = DeleteParam.newBuilder()
                .withCollectionName("coll")
                .withExpr("id > 0")
                .build();

        assertNull(param.getDatabaseName());
        assertEquals("", param.getPartitionName());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> DeleteParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> DeleteParam.newBuilder()
                        .withCollectionName("")
                        .withExpr("id > 0")
                        .build());
    }

    @Test
    void emptyExprIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> DeleteParam.newBuilder()
                        .withCollectionName("coll")
                        .withExpr("")
                        .build());
    }

    @Test
    void missingExprIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> DeleteParam.newBuilder().withCollectionName("coll").build());
    }

    @Test
    void nullExprIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> DeleteParam.newBuilder().withCollectionName("coll").withExpr(null));
    }

    @Test
    void nullPartitionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> DeleteParam.newBuilder().withCollectionName("coll").withPartitionName(null));
    }
}

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
import io.milvus.param.collection.DescribeCollectionParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class DescribeCollectionParamTest {

    @Test
    void builderSetsAllFields() {
        DescribeCollectionParam param = DescribeCollectionParam.newBuilder()
                .withCollectionName("coll")
                .withDatabaseName("db")
                .build();

        assertEquals("coll", param.getCollectionName());
        assertEquals("db", param.getDatabaseName());
    }

    @Test
    void databaseNameDefaultsToNull() {
        DescribeCollectionParam param = DescribeCollectionParam.newBuilder()
                .withCollectionName("coll")
                .build();

        assertNull(param.getDatabaseName());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> DescribeCollectionParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> DescribeCollectionParam.newBuilder().withCollectionName("").build());
    }

    @Test
    void missingCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> DescribeCollectionParam.newBuilder().build());
    }
}

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
import io.milvus.param.index.DescribeIndexParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class DescribeIndexParamTest {

    @Test
    void builderSetsAllFields() {
        DescribeIndexParam param = DescribeIndexParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withIndexName("idx")
                .withFieldName("vector")
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals("idx", param.getIndexName());
        assertEquals("vector", param.getFieldName());
    }

    @Test
    void builderDefaults() {
        DescribeIndexParam param = DescribeIndexParam.newBuilder()
                .withCollectionName("coll")
                .build();

        assertNull(param.getDatabaseName());
        assertEquals("", param.getIndexName());
        assertEquals("", param.getFieldName());
    }

    @Test
    void withNullIndexNameIsAllowed() {
        DescribeIndexParam param = DescribeIndexParam.newBuilder()
                .withCollectionName("coll")
                .withIndexName(null)
                .build();

        assertNull(param.getIndexName());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> DescribeIndexParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> DescribeIndexParam.newBuilder().withCollectionName("").build());
    }

    @Test
    void missingCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> DescribeIndexParam.newBuilder().build());
    }
}

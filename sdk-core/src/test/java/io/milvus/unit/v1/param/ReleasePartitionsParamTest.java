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
import io.milvus.param.partition.ReleasePartitionsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("unit")
class ReleasePartitionsParamTest {
    @Test
    void buildAndGet() {
        ReleasePartitionsParam param = ReleasePartitionsParam.newBuilder()
                .withDatabaseName("db1")
                .withCollectionName("coll1")
                .withPartitionNames(Arrays.asList("p1", "p2"))
                .build();

        assertEquals("db1", param.getDatabaseName());
        assertEquals("coll1", param.getCollectionName());
        assertEquals(Arrays.asList("p1", "p2"), param.getPartitionNames());
    }

    @Test
    void databaseNameIsOptional() {
        ReleasePartitionsParam param = ReleasePartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .build();

        assertNull(param.getDatabaseName());
    }

    @Test
    void addPartitionNameDeduplicates() {
        ReleasePartitionsParam param = ReleasePartitionsParam.newBuilder()
                .withCollectionName("coll1")
                .addPartitionName("p1")
                .addPartitionName("p1")
                .build();

        assertEquals(Collections.singletonList("p1"), param.getPartitionNames());
    }

    @Test
    void buildFailsWhenRequiredFieldMissing() {
        assertThrows(ParamException.class, () -> ReleasePartitionsParam.newBuilder()
                .withCollectionName("coll1").build());
        assertThrows(ParamException.class, () -> ReleasePartitionsParam.newBuilder()
                .withCollectionName("coll1").withPartitionNames(Collections.emptyList()).build());
    }

    @Test
    void withMethodsRejectNull() {
        assertThrows(IllegalArgumentException.class, () ->
                ReleasePartitionsParam.newBuilder().withCollectionName(null));
        assertThrows(IllegalArgumentException.class, () ->
                ReleasePartitionsParam.newBuilder().withPartitionNames(null));
        assertThrows(IllegalArgumentException.class, () ->
                ReleasePartitionsParam.newBuilder().addPartitionName(null));
    }
}

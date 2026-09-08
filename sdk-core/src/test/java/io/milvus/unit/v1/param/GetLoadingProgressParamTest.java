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
import io.milvus.param.collection.GetLoadingProgressParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class GetLoadingProgressParamTest {

    @Test
    void builderSetsAllFields() {
        List<String> partitions = Arrays.asList("p1", "p2");

        GetLoadingProgressParam param = GetLoadingProgressParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionName("coll")
                .withPartitionNames(partitions)
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals("coll", param.getCollectionName());
        assertEquals(partitions, param.getPartitionNames());
    }

    @Test
    void builderDefaults() {
        GetLoadingProgressParam param = GetLoadingProgressParam.newBuilder()
                .withCollectionName("coll")
                .build();

        assertNull(param.getDatabaseName());
        assertTrue(param.getPartitionNames().isEmpty());
    }

    @Test
    void addPartitionNameAccumulatesAndDeDuplicates() {
        GetLoadingProgressParam param = GetLoadingProgressParam.newBuilder()
                .withCollectionName("coll")
                .addPartitionName("a")
                .addPartitionName("a")
                .addPartitionName("b")
                .build();

        assertEquals(Arrays.asList("a", "b"), param.getPartitionNames());
    }

    @Test
    void nullCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> GetLoadingProgressParam.newBuilder().withCollectionName(null));
    }

    @Test
    void emptyCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> GetLoadingProgressParam.newBuilder().withCollectionName("").build());
    }

    @Test
    void missingCollectionNameIsRejectedByBuild() {
        assertThrows(ParamException.class, () -> GetLoadingProgressParam.newBuilder().build());
    }

    @Test
    void nullPartitionNamesIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> GetLoadingProgressParam.newBuilder().withCollectionName("coll").withPartitionNames(null));
    }

    @Test
    void nullAddPartitionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> GetLoadingProgressParam.newBuilder().withCollectionName("coll").addPartitionName(null));
    }
}

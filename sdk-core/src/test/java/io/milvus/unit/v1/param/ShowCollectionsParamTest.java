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
import io.milvus.grpc.ShowType;
import io.milvus.param.collection.ShowCollectionsParam;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ShowCollectionsParamTest {

    @Test
    void builderSetsAllFields() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .withDatabaseName("db")
                .withCollectionNames(Arrays.asList("a", "b"))
                .build();

        assertEquals("db", param.getDatabaseName());
        assertEquals(Arrays.asList("a", "b"), param.getCollectionNames());
        assertEquals(ShowType.InMemory, param.getShowType());
    }

    @Test
    void defaultsToAllShowTypeWithoutCollections() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .withDatabaseName("db")
                .build();

        assertEquals("db", param.getDatabaseName());
        assertTrue(param.getCollectionNames().isEmpty());
        assertEquals(ShowType.All, param.getShowType());
    }

    @Test
    void withShowTypeIsRespectedWhenNoCollections() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .withShowType(ShowType.InMemory)
                .build();

        assertEquals(ShowType.InMemory, param.getShowType());
    }

    @Test
    void buildOverridesShowTypeToInMemoryWhenCollectionsGiven() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .withShowType(ShowType.All)
                .addCollectionName("a")
                .build();

        assertEquals(ShowType.InMemory, param.getShowType());
    }

    @Test
    void addCollectionNameAccumulatesAndDeDuplicates() {
        ShowCollectionsParam param = ShowCollectionsParam.newBuilder()
                .addCollectionName("a")
                .addCollectionName("a")
                .addCollectionName("b")
                .build();

        assertEquals(Arrays.asList("a", "b"), param.getCollectionNames());
    }

    @Test
    void nullCollectionNamesIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ShowCollectionsParam.newBuilder().withCollectionNames(null));
    }

    @Test
    void nullAddCollectionNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> ShowCollectionsParam.newBuilder().addCollectionName(null));
    }

    @Test
    void emptyCollectionNameInsideListIsRejectedByBuild() {
        assertThrows(ParamException.class,
                () -> ShowCollectionsParam.newBuilder().withCollectionNames(Arrays.asList("", "b")).build());
    }
}

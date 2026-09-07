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

package io.milvus.unit.v2.service.collection;

import io.milvus.v2.service.collection.request.BatchDescribeCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class BatchDescribeCollectionReqTest {

    @Test
    void builderBuildsWithAllFields() {
        List<String> names = Arrays.asList("coll1", "coll2");
        List<Long> ids = Arrays.asList(1L, 2L);
        BatchDescribeCollectionReq request = BatchDescribeCollectionReq.builder()
                .databaseName("db")
                .collectionNames(names)
                .collectionIds(ids)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals(names, request.getCollectionNames());
        assertEquals(ids, request.getCollectionIds());
    }

    @Test
    void settersUpdateGetters() {
        BatchDescribeCollectionReq request = BatchDescribeCollectionReq.builder().build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        List<String> names = Arrays.asList("a", "b");
        request.setCollectionNames(names);
        assertEquals(names, request.getCollectionNames());

        List<Long> ids = Arrays.asList(10L, 20L);
        request.setCollectionIds(ids);
        assertEquals(ids, request.getCollectionIds());
    }

    @Test
    void defaultsAreNull() {
        BatchDescribeCollectionReq request = BatchDescribeCollectionReq.builder().build();
        assertNull(request.getDatabaseName());
        assertNull(request.getCollectionNames());
        assertNull(request.getCollectionIds());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(BatchDescribeCollectionReq.builder());
    }

    @Test
    void toStringContainsFields() {
        BatchDescribeCollectionReq request = BatchDescribeCollectionReq.builder()
                .collectionNames(Arrays.asList("coll1", "coll2"))
                .build();
        assertTrue(request.toString().contains("coll1"));
    }
}

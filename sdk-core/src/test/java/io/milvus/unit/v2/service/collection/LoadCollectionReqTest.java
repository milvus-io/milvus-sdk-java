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

import io.milvus.v2.service.collection.request.LoadCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class LoadCollectionReqTest {

    @Test
    void builderBuildsWithAllFields() {
        List<String> loadFields = Arrays.asList("id", "vector");
        List<String> resourceGroups = Arrays.asList("rg1", "rg2");
        LoadCollectionReq request = LoadCollectionReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .numReplicas(3)
                .sync(true)
                .timeout(120000L)
                .refresh(true)
                .loadFields(loadFields)
                .skipLoadDynamicField(true)
                .resourceGroups(resourceGroups)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(3, request.getNumReplicas());
        assertTrue(request.getSync());
        assertFalse(request.getAsync());
        assertEquals(120000L, request.getTimeout());
        assertTrue(request.getRefresh());
        assertEquals(loadFields, request.getLoadFields());
        assertTrue(request.getSkipLoadDynamicField());
        assertEquals(resourceGroups, request.getResourceGroups());
    }

    @Test
    void asyncBuilderTogglesSync() {
        LoadCollectionReq request = LoadCollectionReq.builder()
                .collectionName("coll")
                .async(true)
                .build();
        assertTrue(request.getAsync());
        assertFalse(request.getSync());

        LoadCollectionReq syncRequest = LoadCollectionReq.builder()
                .collectionName("coll")
                .sync(true)
                .build();
        assertTrue(syncRequest.getSync());
        assertFalse(syncRequest.getAsync());
    }

    @Test
    void defaultsApply() {
        LoadCollectionReq request = LoadCollectionReq.builder().collectionName("coll").build();

        assertEquals(1, request.getNumReplicas());
        assertFalse(request.getAsync());
        assertTrue(request.getSync());
        assertEquals(60000L, request.getTimeout());
        assertFalse(request.getRefresh());
        assertTrue(request.getLoadFields().isEmpty());
        assertFalse(request.getSkipLoadDynamicField());
        assertTrue(request.getResourceGroups().isEmpty());
    }

    @Test
    void settersUpdateGetters() {
        LoadCollectionReq request = LoadCollectionReq.builder().build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setNumReplicas(2);
        assertEquals(2, request.getNumReplicas());

        request.setAsync(true);
        assertTrue(request.getAsync());
        assertFalse(request.getSync());

        request.setSync(true);
        assertTrue(request.getSync());
        assertFalse(request.getAsync());

        request.setTimeout(5000L);
        assertEquals(5000L, request.getTimeout());

        request.setRefresh(true);
        assertTrue(request.getRefresh());

        request.setSkipLoadDynamicField(true);
        assertTrue(request.getSkipLoadDynamicField());

        List<String> fields = Arrays.asList("f1", "f2");
        request.setLoadFields(fields);
        assertEquals(fields, request.getLoadFields());

        List<String> groups = Arrays.asList("rg1");
        request.setResourceGroups(groups);
        assertEquals(groups, request.getResourceGroups());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(LoadCollectionReq.builder());
    }

    @Test
    void toStringContainsFields() {
        LoadCollectionReq request = LoadCollectionReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }

    @Test
    void databaseNameIsNullByDefault() {
        assertNull(LoadCollectionReq.builder().build().getDatabaseName());
    }
}

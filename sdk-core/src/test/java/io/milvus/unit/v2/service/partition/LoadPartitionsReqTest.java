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

package io.milvus.unit.v2.service.partition;

import io.milvus.v2.service.partition.request.LoadPartitionsReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class LoadPartitionsReqTest {
    @Test
    void builderBuildsAllFields() {
        List<String> partitionNames = Arrays.asList("p1", "p2");
        List<String> loadFields = Collections.singletonList("field");
        List<String> resourceGroups = Collections.singletonList("rg");

        LoadPartitionsReq request = LoadPartitionsReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .partitionNames(partitionNames)
                .numReplicas(3)
                .sync(false)
                .timeout(120000L)
                .refresh(true)
                .loadFields(loadFields)
                .skipLoadDynamicField(true)
                .resourceGroups(resourceGroups)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(partitionNames, request.getPartitionNames());
        assertEquals(Integer.valueOf(3), request.getNumReplicas());
        assertEquals(Boolean.FALSE, request.getSync());
        assertEquals(Long.valueOf(120000L), request.getTimeout());
        assertEquals(Boolean.TRUE, request.getRefresh());
        assertEquals(loadFields, request.getLoadFields());
        assertEquals(Boolean.TRUE, request.getSkipLoadDynamicField());
        assertEquals(resourceGroups, request.getResourceGroups());
    }

    @Test
    void unsetFieldsUseBuilderDefaults() {
        LoadPartitionsReq request = LoadPartitionsReq.builder().build();

        assertNull(request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertTrue(request.getPartitionNames().isEmpty());
        assertEquals(Integer.valueOf(1), request.getNumReplicas());
        assertTrue(request.getSync());
        assertEquals(Long.valueOf(60000L), request.getTimeout());
        assertFalse(request.getRefresh());
        assertTrue(request.getLoadFields().isEmpty());
        assertFalse(request.getSkipLoadDynamicField());
        assertTrue(request.getResourceGroups().isEmpty());
    }

    @Test
    void settersUpdateFields() {
        LoadPartitionsReq request = LoadPartitionsReq.builder().build();

        request.setDatabaseName("db");
        request.setCollectionName("coll");
        request.setPartitionNames(Collections.singletonList("p1"));
        request.setNumReplicas(5);
        request.setSync(false);
        request.setTimeout(30000L);
        request.setRefresh(true);
        request.setLoadFields(Collections.singletonList("field"));
        request.setSkipLoadDynamicField(true);
        request.setResourceGroups(Collections.singletonList("rg"));

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(Collections.singletonList("p1"), request.getPartitionNames());
        assertEquals(Integer.valueOf(5), request.getNumReplicas());
        assertEquals(Boolean.FALSE, request.getSync());
        assertEquals(Long.valueOf(30000L), request.getTimeout());
        assertEquals(Boolean.TRUE, request.getRefresh());
        assertEquals(Collections.singletonList("field"), request.getLoadFields());
        assertEquals(Boolean.TRUE, request.getSkipLoadDynamicField());
        assertEquals(Collections.singletonList("rg"), request.getResourceGroups());
    }

    @Test
    void boundaryValuesAreAccepted() {
        LoadPartitionsReq request = LoadPartitionsReq.builder()
                .numReplicas(0)
                .timeout(0L)
                .build();

        assertEquals(Integer.valueOf(0), request.getNumReplicas());
        assertEquals(Long.valueOf(0L), request.getTimeout());
    }
}

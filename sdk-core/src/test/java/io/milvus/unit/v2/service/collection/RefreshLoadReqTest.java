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

import io.milvus.v2.service.collection.request.RefreshLoadReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RefreshLoadReqTest {

    @Test
    void builderBuildsWithAllFields() {
        RefreshLoadReq request = RefreshLoadReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .sync(true)
                .timeout(30000L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertTrue(request.getSync());
        assertFalse(request.getAsync());
        assertEquals(30000L, request.getTimeout());
    }

    @Test
    void asyncBuilderTogglesSync() {
        RefreshLoadReq request = RefreshLoadReq.builder()
                .collectionName("coll")
                .async(true)
                .build();
        assertTrue(request.getAsync());
        assertFalse(request.getSync());
    }

    @Test
    void defaultsApply() {
        RefreshLoadReq request = RefreshLoadReq.builder().build();
        assertNull(request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertTrue(request.getAsync());
        assertTrue(request.getSync());
        assertEquals(60000L, request.getTimeout());
    }

    @Test
    void settersUpdateGetters() {
        RefreshLoadReq request = RefreshLoadReq.builder().build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setAsync(true);
        assertTrue(request.getAsync());
        assertFalse(request.getSync());

        request.setSync(true);
        assertTrue(request.getSync());
        assertFalse(request.getAsync());

        request.setTimeout(5000L);
        assertEquals(5000L, request.getTimeout());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(RefreshLoadReq.builder());
    }

    @Test
    void toStringContainsFields() {
        RefreshLoadReq request = RefreshLoadReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }
}

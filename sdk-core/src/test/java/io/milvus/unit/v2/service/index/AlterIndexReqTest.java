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

package io.milvus.unit.v2.service.index;

import io.milvus.v2.service.index.request.AlterIndexReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AlterIndexReqTest {

    @Test
    void builderBuildsWithAllFields() {
        Map<String, String> properties = Collections.singletonMap("mmap.enabled", "true");
        AlterIndexReq request = AlterIndexReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .indexName("idx")
                .properties(properties)
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("idx", request.getIndexName());
        assertEquals(properties, request.getProperties());
    }

    @Test
    void settersUpdateGetters() {
        AlterIndexReq request = AlterIndexReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setIndexName("i2");
        assertEquals("i2", request.getIndexName());

        Map<String, String> props = Collections.singletonMap("k", "v");
        request.setProperties(props);
        assertEquals(props, request.getProperties());
    }

    @Test
    void propertiesDefaultToEmptyMap() {
        AlterIndexReq request = AlterIndexReq.builder().build();
        assertNotNull(request.getProperties());
        assertTrue(request.getProperties().isEmpty());
        assertNull(request.getCollectionName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(AlterIndexReq.builder());
    }

    @Test
    void toStringContainsFields() {
        AlterIndexReq request = AlterIndexReq.builder()
                .collectionName("coll")
                .indexName("idx")
                .build();
        assertTrue(request.toString().contains("coll"));
    }
}

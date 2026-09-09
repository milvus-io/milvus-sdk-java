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

import io.milvus.v2.service.collection.request.AlterCollectionFieldReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AlterCollectionFieldReqTest {

    @Test
    void builderBuildsWithAllFields() {
        Map<String, String> properties = Collections.singletonMap("mmap.enabled", "false");
        AlterCollectionFieldReq request = AlterCollectionFieldReq.builder()
                .collectionName("coll")
                .fieldName("f")
                .databaseName("db")
                .properties(properties)
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("f", request.getFieldName());
        assertEquals("db", request.getDatabaseName());
        assertEquals(properties, request.getProperties());
    }

    @Test
    void propertyAddsSingleEntry() {
        AlterCollectionFieldReq request = AlterCollectionFieldReq.builder()
                .collectionName("coll")
                .fieldName("f")
                .property("a", "1")
                .property("b", "2")
                .build();

        assertEquals("1", request.getProperties().get("a"));
        assertEquals("2", request.getProperties().get("b"));
    }

    @Test
    void settersUpdateGetters() {
        AlterCollectionFieldReq request = AlterCollectionFieldReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setFieldName("f2");
        assertEquals("f2", request.getFieldName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());
    }

    @Test
    void propertiesDefaultToEmptyMap() {
        AlterCollectionFieldReq request = AlterCollectionFieldReq.builder().build();
        assertNotNull(request.getProperties());
        assertTrue(request.getProperties().isEmpty());
        assertNull(request.getCollectionName());
        assertNull(request.getFieldName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(AlterCollectionFieldReq.builder());
    }

    @Test
    void toStringContainsFields() {
        AlterCollectionFieldReq request = AlterCollectionFieldReq.builder()
                .collectionName("coll")
                .fieldName("f")
                .build();
        assertTrue(request.toString().contains("coll"));
    }
}

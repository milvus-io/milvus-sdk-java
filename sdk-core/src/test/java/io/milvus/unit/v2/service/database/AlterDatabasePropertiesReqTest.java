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

package io.milvus.unit.v2.service.database;

import io.milvus.v2.service.database.request.AlterDatabasePropertiesReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class AlterDatabasePropertiesReqTest {

    @Test
    void builderBuildsWithAllFields() {
        Map<String, String> properties = Collections.singletonMap("database.replica", "3");
        AlterDatabasePropertiesReq request = AlterDatabasePropertiesReq.builder()
                .databaseName("db")
                .properties(properties)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals(properties, request.getProperties());
    }

    @Test
    void propertyAddsSingleEntry() {
        AlterDatabasePropertiesReq request = AlterDatabasePropertiesReq.builder()
                .databaseName("db")
                .property("key1", "value1")
                .property("key2", "value2")
                .build();

        assertEquals("value1", request.getProperties().get("key1"));
        assertEquals("value2", request.getProperties().get("key2"));
    }

    @Test
    void settersUpdateGetters() {
        AlterDatabasePropertiesReq request = AlterDatabasePropertiesReq.builder().build();

        request.setDatabaseName("other");
        assertEquals("other", request.getDatabaseName());

        Map<String, String> props = Collections.singletonMap("key", "value");
        request.setProperties(props);
        assertEquals(props, request.getProperties());
    }

    @Test
    void propertiesDefaultToEmptyMap() {
        AlterDatabasePropertiesReq request = AlterDatabasePropertiesReq.builder().databaseName("db").build();
        assertNotNull(request.getProperties());
        assertTrue(request.getProperties().isEmpty());
    }

    @Test
    void toStringContainsFields() {
        AlterDatabasePropertiesReq request = AlterDatabasePropertiesReq.builder().databaseName("db").build();
        assertTrue(request.toString().contains("db"));
    }
}

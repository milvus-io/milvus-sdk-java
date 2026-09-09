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

import io.milvus.v2.service.database.request.DropDatabasePropertiesReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DropDatabasePropertiesReqTest {

    @Test
    void builderBuildsWithAllFields() {
        List<String> propertyKeys = Arrays.asList("replica.num", "ttl");
        DropDatabasePropertiesReq request = DropDatabasePropertiesReq.builder()
                .databaseName("db")
                .propertyKeys(propertyKeys)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals(propertyKeys, request.getPropertyKeys());
    }

    @Test
    void settersUpdateGetters() {
        DropDatabasePropertiesReq request = DropDatabasePropertiesReq.builder().build();

        request.setDatabaseName("other");
        assertEquals("other", request.getDatabaseName());

        List<String> keys = Arrays.asList("a", "b");
        request.setPropertyKeys(keys);
        assertEquals(keys, request.getPropertyKeys());
    }

    @Test
    void propertyKeysDefaultToEmptyList() {
        DropDatabasePropertiesReq request = DropDatabasePropertiesReq.builder().databaseName("db").build();
        assertNotNull(request.getPropertyKeys());
        assertTrue(request.getPropertyKeys().isEmpty());
    }

    @Test
    void toStringContainsFields() {
        DropDatabasePropertiesReq request = DropDatabasePropertiesReq.builder().databaseName("db").build();
        assertTrue(request.toString().contains("db"));
    }
}

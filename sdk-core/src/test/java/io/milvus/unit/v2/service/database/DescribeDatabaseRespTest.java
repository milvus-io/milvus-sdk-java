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

import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeDatabaseRespTest {

    @Test
    void builderBuildsWithAllFields() {
        Map<String, String> properties = new HashMap<>();
        properties.put("replica.num", "2");
        DescribeDatabaseResp response = DescribeDatabaseResp.builder()
                .databaseName("db")
                .properties(properties)
                .build();

        assertEquals("db", response.getDatabaseName());
        assertEquals(properties, response.getProperties());
    }

    @Test
    void settersUpdateGetters() {
        DescribeDatabaseResp response = DescribeDatabaseResp.builder().build();

        response.setDatabaseName("other");
        assertEquals("other", response.getDatabaseName());

        Map<String, String> props = Collections.singletonMap("key", "value");
        response.setProperties(props);
        assertEquals(props, response.getProperties());
    }

    @Test
    void propertiesDefaultToEmptyMap() {
        DescribeDatabaseResp response = DescribeDatabaseResp.builder().databaseName("db").build();
        assertNotNull(response.getProperties());
        assertTrue(response.getProperties().isEmpty());
    }

    @Test
    void toStringContainsFields() {
        DescribeDatabaseResp response = DescribeDatabaseResp.builder().databaseName("db").build();
        assertTrue(response.toString().contains("db"));
    }
}

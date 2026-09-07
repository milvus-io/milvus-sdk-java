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

import io.milvus.v2.service.database.response.ListDatabasesResp;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ListDatabasesRespTest {

    @Test
    void builderBuildsWithAllFields() {
        List<String> names = Arrays.asList("db1", "db2");
        ListDatabasesResp response = ListDatabasesResp.builder()
                .databaseNames(names)
                .build();

        assertEquals(names, response.getDatabaseNames());
    }

    @Test
    void setterUpdatesGetter() {
        ListDatabasesResp response = ListDatabasesResp.builder().build();
        List<String> names = Arrays.asList("a", "b");
        response.setDatabaseNames(names);
        assertEquals(names, response.getDatabaseNames());
    }

    @Test
    void databaseNamesDefaultToEmptyList() {
        ListDatabasesResp response = ListDatabasesResp.builder().build();
        assertNotNull(response.getDatabaseNames());
        assertTrue(response.getDatabaseNames().isEmpty());
    }

    @Test
    void toStringContainsFields() {
        ListDatabasesResp response = ListDatabasesResp.builder()
                .databaseNames(Arrays.asList("db1", "db2"))
                .build();
        assertTrue(response.toString().contains("db1"));
    }
}

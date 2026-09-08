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

import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeDatabaseReqTest {

    @Test
    void builderBuildsWithAllFields() {
        DescribeDatabaseReq request = DescribeDatabaseReq.builder()
                .databaseName("db")
                .build();

        assertEquals("db", request.getDatabaseName());
    }

    @Test
    void databaseNameIsNullByDefault() {
        DescribeDatabaseReq request = DescribeDatabaseReq.builder().build();
        assertNull(request.getDatabaseName());
    }

    @Test
    void setterUpdatesGetter() {
        DescribeDatabaseReq request = DescribeDatabaseReq.builder().build();
        request.setDatabaseName("other");
        assertEquals("other", request.getDatabaseName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(DescribeDatabaseReq.builder());
        assertNotNull(DescribeDatabaseReq.builder().databaseName("db").build());
    }

    @Test
    void toStringContainsFields() {
        DescribeDatabaseReq request = DescribeDatabaseReq.builder().databaseName("db").build();
        assertTrue(request.toString().contains("db"));
    }
}

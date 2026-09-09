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

import io.milvus.v2.service.index.request.ListIndexesReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class ListIndexesReqTest {

    @Test
    void builderBuildsWithAllFields() {
        ListIndexesReq request = ListIndexesReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .fieldName("vector")
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("vector", request.getFieldName());
    }

    @Test
    void defaultsApply() {
        ListIndexesReq request = ListIndexesReq.builder().collectionName("coll").build();
        assertNull(request.getDatabaseName());
        assertNull(request.getFieldName());
    }

    @Test
    void collectionNameCannotBeNull() {
        assertThrows(IllegalArgumentException.class,
                () -> ListIndexesReq.builder().collectionName(null));
        assertThrows(IllegalArgumentException.class,
                () -> ListIndexesReq.builder().build());
    }

    @Test
    void setterRejectsNullCollectionName() {
        ListIndexesReq request = ListIndexesReq.builder().collectionName("coll").build();
        assertThrows(IllegalArgumentException.class, () -> request.setCollectionName(null));
    }

    @Test
    void settersUpdateGetters() {
        ListIndexesReq request = ListIndexesReq.builder().collectionName("coll").build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setFieldName("f2");
        assertEquals("f2", request.getFieldName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(ListIndexesReq.builder());
    }

    @Test
    void toStringContainsFields() {
        ListIndexesReq request = ListIndexesReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }
}

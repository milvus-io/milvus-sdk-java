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

import io.milvus.v2.service.collection.request.DropCollectionFieldReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DropCollectionFieldReqTest {

    @Test
    void builderBuildsWithAllFields() {
        DropCollectionFieldReq request = DropCollectionFieldReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .fieldName("f")
                .fieldId(42L)
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("f", request.getFieldName());
        assertEquals(42L, request.getFieldId());
    }

    @Test
    void settersUpdateGetters() {
        DropCollectionFieldReq request = DropCollectionFieldReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setFieldName("f2");
        assertEquals("f2", request.getFieldName());

        request.setFieldId(7L);
        assertEquals(7L, request.getFieldId());
    }

    @Test
    void defaultsAreEmptyStrings() {
        DropCollectionFieldReq request = DropCollectionFieldReq.builder().build();
        assertEquals("", request.getCollectionName());
        assertEquals("", request.getDatabaseName());
        assertEquals("", request.getFieldName());
        assertNull(request.getFieldId());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(DropCollectionFieldReq.builder());
    }

    @Test
    void toStringContainsFields() {
        DropCollectionFieldReq request = DropCollectionFieldReq.builder()
                .collectionName("coll")
                .fieldName("f")
                .build();
        assertTrue(request.toString().contains("coll"));
    }
}

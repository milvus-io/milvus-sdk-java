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

import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DescribeCollectionReqTest {

    @Test
    void builderBuildsWithAllFields() {
        DescribeCollectionReq request = DescribeCollectionReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .collectionId(12345L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(12345L, request.getCollectionId());
    }

    @Test
    void settersUpdateGetters() {
        DescribeCollectionReq request = DescribeCollectionReq.builder().build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setCollectionId(999L);
        assertEquals(999L, request.getCollectionId());
    }

    @Test
    void defaultsAreNull() {
        DescribeCollectionReq request = DescribeCollectionReq.builder().build();
        assertNull(request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertNull(request.getCollectionId());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(DescribeCollectionReq.builder());
    }

    @Test
    void toStringContainsFields() {
        DescribeCollectionReq request = DescribeCollectionReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }
}

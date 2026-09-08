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

import io.milvus.v2.service.collection.request.DropCollectionFunctionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DropCollectionFunctionReqTest {

    @Test
    void builderBuildsWithAllFields() {
        DropCollectionFunctionReq request = DropCollectionFunctionReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .functionName("bm25_fn")
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("bm25_fn", request.getFunctionName());
    }

    @Test
    void settersUpdateGetters() {
        DropCollectionFunctionReq request = DropCollectionFunctionReq.builder().build();

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());
    }

    @Test
    void defaultsAreEmptyStrings() {
        DropCollectionFunctionReq request = DropCollectionFunctionReq.builder().build();
        assertEquals("", request.getCollectionName());
        assertEquals("", request.getDatabaseName());
        assertEquals("", request.getFunctionName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(DropCollectionFunctionReq.builder());
    }

    @Test
    void toStringContainsFields() {
        DropCollectionFunctionReq request = DropCollectionFunctionReq.builder()
                .collectionName("coll")
                .functionName("fn")
                .build();
        assertTrue(request.toString().contains("coll"));
    }
}

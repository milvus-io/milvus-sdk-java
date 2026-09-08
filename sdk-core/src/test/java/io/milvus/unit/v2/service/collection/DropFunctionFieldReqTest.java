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

import io.milvus.v2.service.collection.request.DropFunctionFieldReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class DropFunctionFieldReqTest {

    @Test
    void builderBuildsWithAllFields() {
        DropFunctionFieldReq request = DropFunctionFieldReq.builder()
                .collectionName("coll")
                .databaseName("db")
                .functionName("fn")
                .build();

        assertEquals("coll", request.getCollectionName());
        assertEquals("db", request.getDatabaseName());
        assertEquals("fn", request.getFunctionName());
    }

    @Test
    void defaultsAreEmptyStrings() {
        DropFunctionFieldReq request = DropFunctionFieldReq.builder().build();
        assertEquals("", request.getCollectionName());
        assertEquals("", request.getDatabaseName());
        assertEquals("", request.getFunctionName());
    }

    @Test
    void settersUpdateGetters() {
        DropFunctionFieldReq request = DropFunctionFieldReq.builder().build();

        request.setCollectionName("coll2");
        request.setDatabaseName("db2");
        request.setFunctionName("fn2");

        assertEquals("coll2", request.getCollectionName());
        assertEquals("db2", request.getDatabaseName());
        assertEquals("fn2", request.getFunctionName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(DropFunctionFieldReq.builder());
    }

    @Test
    void toStringContainsFields() {
        DropFunctionFieldReq request = DropFunctionFieldReq.builder()
                .collectionName("coll")
                .functionName("fn")
                .build();
        assertTrue(request.toString().contains("coll"));
        assertTrue(request.toString().contains("fn"));
    }
}

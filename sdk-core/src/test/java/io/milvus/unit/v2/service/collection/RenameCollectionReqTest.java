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

import io.milvus.v2.service.collection.request.RenameCollectionReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class RenameCollectionReqTest {

    @Test
    void builderBuildsWithAllFields() {
        RenameCollectionReq request = RenameCollectionReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .newCollectionName("coll_new")
                .targetDbName("db_target")
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("coll_new", request.getNewCollectionName());
        assertEquals("db_target", request.getTargetDbName());
    }

    @Test
    void settersUpdateGetters() {
        RenameCollectionReq request = RenameCollectionReq.builder().build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        request.setNewCollectionName("coll_new2");
        assertEquals("coll_new2", request.getNewCollectionName());

        request.setTargetDbName("db_target2");
        assertEquals("db_target2", request.getTargetDbName());
    }

    @Test
    void defaultsAreNull() {
        RenameCollectionReq request = RenameCollectionReq.builder().build();
        assertNull(request.getDatabaseName());
        assertNull(request.getCollectionName());
        assertNull(request.getNewCollectionName());
        assertNull(request.getTargetDbName());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(RenameCollectionReq.builder());
    }

    @Test
    void toStringContainsFields() {
        RenameCollectionReq request = RenameCollectionReq.builder()
                .collectionName("coll")
                .newCollectionName("coll_new")
                .build();
        assertTrue(request.toString().contains("coll_new"));
    }
}

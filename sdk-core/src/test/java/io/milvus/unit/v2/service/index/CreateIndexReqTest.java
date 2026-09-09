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

import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.index.request.CreateIndexReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class CreateIndexReqTest {

    private List<IndexParam> buildIndexParams() {
        return Arrays.asList(
                IndexParam.builder().fieldName("vector").indexType(IndexParam.IndexType.FLAT).build(),
                IndexParam.builder().fieldName("text").indexType(IndexParam.IndexType.INVERTED).build());
    }

    @Test
    void builderBuildsWithAllFields() {
        List<IndexParam> params = buildIndexParams();
        CreateIndexReq request = CreateIndexReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .indexParams(params)
                .sync(false)
                .timeout(30000L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals(params, request.getIndexParams());
        assertFalse(request.getSync());
        assertEquals(30000L, request.getTimeout());
    }

    @Test
    void defaultsApply() {
        CreateIndexReq request = CreateIndexReq.builder().collectionName("coll").build();
        assertNull(request.getDatabaseName());
        assertNull(request.getIndexParams());
        assertTrue(request.getSync());
        assertEquals(60000L, request.getTimeout());
    }

    @Test
    void collectionNameCannotBeNull() {
        assertThrows(IllegalArgumentException.class,
                () -> CreateIndexReq.builder().collectionName(null));
        assertThrows(IllegalArgumentException.class,
                () -> CreateIndexReq.builder().build());
    }

    @Test
    void setterRejectsNullCollectionName() {
        CreateIndexReq request = CreateIndexReq.builder().collectionName("coll").build();
        assertThrows(IllegalArgumentException.class, () -> request.setCollectionName(null));
    }

    @Test
    void settersUpdateGetters() {
        CreateIndexReq request = CreateIndexReq.builder().collectionName("coll").build();

        request.setDatabaseName("db2");
        assertEquals("db2", request.getDatabaseName());

        request.setCollectionName("coll2");
        assertEquals("coll2", request.getCollectionName());

        List<IndexParam> params = buildIndexParams();
        request.setIndexParams(params);
        assertEquals(params, request.getIndexParams());

        request.setSync(true);
        assertTrue(request.getSync());

        request.setTimeout(1000L);
        assertEquals(1000L, request.getTimeout());
    }

    @Test
    void builderFactoryReturnsBuilder() {
        assertNotNull(CreateIndexReq.builder());
    }

    @Test
    void toStringContainsFields() {
        CreateIndexReq request = CreateIndexReq.builder().collectionName("coll").build();
        assertTrue(request.toString().contains("coll"));
    }
}

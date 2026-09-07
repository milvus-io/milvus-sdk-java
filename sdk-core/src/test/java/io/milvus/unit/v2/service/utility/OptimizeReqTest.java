/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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

package io.milvus.unit.v2.service.utility;

import io.milvus.v2.service.utility.request.OptimizeReq;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("unit")
class OptimizeReqTest {
    @Test
    void builderBuildsAllFields() {
        OptimizeReq request = OptimizeReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .targetSize("512MB")
                .async(true)
                .timeout(10000L)
                .build();

        assertEquals("db", request.getDatabaseName());
        assertEquals("coll", request.getCollectionName());
        assertEquals("512MB", request.getTargetSize());
        assertTrue(request.isAsync());
        assertEquals(Long.valueOf(10000L), request.getTimeout());
    }

    @Test
    void unsetFieldsUseDefaults() {
        OptimizeReq request = OptimizeReq.builder()
                .collectionName("coll")
                .build();

        assertNull(request.getDatabaseName());
        assertNull(request.getTargetSize());
        assertFalse(request.isAsync());
        assertNull(request.getTimeout());
    }

    @Test
    void buildRejectsNullCollectionName() {
        assertThrows(IllegalArgumentException.class, () -> OptimizeReq.builder().build());
    }

    @Test
    void buildRejectsEmptyCollectionName() {
        assertThrows(IllegalArgumentException.class, () -> OptimizeReq.builder().collectionName("").build());
    }

    @Test
    void asyncBoundaryValues() {
        assertTrue(OptimizeReq.builder().collectionName("coll").async(true).build().isAsync());
        assertFalse(OptimizeReq.builder().collectionName("coll").async(false).build().isAsync());
    }

    @Test
    void toStringContainsFields() {
        OptimizeReq request = OptimizeReq.builder()
                .databaseName("db")
                .collectionName("coll")
                .targetSize("1GB")
                .async(true)
                .timeout(1L)
                .build();

        String text = request.toString();
        assertEquals("OptimizeReq{databaseName='db', collectionName='coll', targetSize='1GB', async=true, timeout=1}", text);
    }
}

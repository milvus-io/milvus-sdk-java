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

package io.milvus.v2.service;

import io.milvus.common.utils.cache.CollectionCacheKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BaseServiceDatabaseNameTest {
    @Test
    void resolvesRpcAndCacheDatabaseNames() {
        TestService service = new TestService();

        assertResolution(service, null, null, "", "default");
        assertResolution(service, null, "request_db", "request_db", "request_db");
        assertResolution(service, "connected_db", null, "connected_db", "connected_db");
        assertResolution(service, "connected_db", "request_db", "request_db", "request_db");
    }

    private static void assertResolution(TestService service, String connectedDb,
                                         String requestDb, String expectedRpcDb,
                                         String expectedCacheDb) {
        service.setCurrentDbName(connectedDb);
        String rpcDb = service.resolve(requestDb);
        assertEquals(expectedRpcDb, rpcDb);
        assertEquals(expectedCacheDb,
                CollectionCacheKey.create("host:19530", rpcDb, "coll").getDatabaseName());
    }

    private static class TestService extends BaseService {
        private String resolve(String databaseName) {
            return actualDbName(databaseName);
        }
    }
}

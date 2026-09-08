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

package io.milvus.unit.common.cache;

import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.grpc.DescribeCollectionResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("unit")
class SchemaCacheDatabaseTest {

    @Test
    void supportsDatabaseInvalidationAndLruEviction() {
        SchemaCache cache = new SchemaCache(2);
        cache.set("host:19530", "db", "one", response(1L));
        cache.set("host:19530", "db", "two", response(2L));
        cache.get("host:19530", "db", "one");
        cache.set("host:19530", "other", "three", response(3L));

        assertNull(cache.get("host:19530", "db", "two"));
        cache.invalidateDb("host:19530", "db");
        assertNull(cache.get("host:19530", "db", "one"));
        assertEquals(3L, cache.get("host:19530", "other", "three").getCollectionID());
        cache.clear();
        assertEquals(0, cache.size());
    }

    private static DescribeCollectionResponse response(long collectionId) {
        return DescribeCollectionResponse.newBuilder().setCollectionID(collectionId).build();
    }
}

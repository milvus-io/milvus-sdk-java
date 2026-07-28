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

package io.milvus.common.utils.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CollectionTsCacheTest {
    @Test
    void isolatesEntriesAndKeepsLatestTimestamp() {
        CollectionTsCache cache = new CollectionTsCache();

        cache.set("HOST:19530", null, "coll", 20L);
        cache.set("host:19530", "default", "coll", 10L);
        cache.set("host:19531", "default", "coll", 30L);
        cache.set("host:19530", "other", "coll", 40L);
        cache.set("https://cloud.example.com", "default", "coll", 50L);

        assertEquals(20L, cache.get("http://host:19530", "default", "coll"));
        assertEquals(30L, cache.get("host:19531", "default", "coll"));
        assertEquals(40L, cache.get("host:19530", "other", "coll"));
        assertEquals(50L, cache.get("cloud.example.com:443", "default", "coll"));
        assertEquals(4, cache.size());
    }

    @Test
    void getsLatestTimestampAcrossEndpointsForLegacyConversion() {
        CollectionTsCache cache = new CollectionTsCache();

        cache.set("host-a:19530", "db", "coll", 100L);
        cache.set("host-b:19530", "db", "coll", 200L);
        cache.set("host-c:19530", "other-db", "coll", 300L);
        cache.set("host-d:19530", "db", "other-coll", 400L);

        assertEquals(200L, cache.getAnyEndpoint("db", "coll"));
        assertEquals(300L, cache.getAnyEndpoint("other-db", "coll"));
        assertEquals(400L, cache.getAnyEndpoint("db", "other-coll"));
        assertEquals(0L, cache.getAnyEndpoint("db", "missing"));

        // Endpoint-aware lookups remain isolated.
        assertEquals(100L, cache.get("host-a:19530", "db", "coll"));
        assertEquals(200L, cache.get("host-b:19530", "db", "coll"));
    }

    @Test
    void ignoresZeroAndSupportsInvalidationAndRename() {
        CollectionTsCache cache = new CollectionTsCache();

        cache.set("host:19530", "db", "zero", 0L);
        assertEquals(0, cache.size());

        cache.set("host:19530", "db", "old", 100L);
        cache.set("host:19530", "db", "new", 200L);
        cache.rename("host:19530", "db", "old", "new");
        assertEquals(0L, cache.get("host:19530", "db", "old"));
        assertEquals(200L, cache.get("host:19530", "db", "new"));

        cache.rename("host:19530", "db", "missing", "new");
        assertEquals(200L, cache.get("host:19530", "db", "new"));

        cache.invalidate("host:19530", "db", "new");
        assertEquals(0, cache.size());
    }

    @Test
    void movesTimestampAcrossDatabases() {
        CollectionTsCache cache = new CollectionTsCache();

        cache.set("host:19530", "source", "old", 100L);
        cache.set("host:19530", "target", "new", 50L);
        cache.rename("host:19530", "source", "old", "target", "new");

        assertEquals(0L, cache.get("host:19530", "source", "old"));
        assertEquals(100L, cache.get("host:19530", "target", "new"));
        assertEquals(1, cache.size());
    }

    @Test
    void retainsEntriesUntilExplicitInvalidation() {
        CollectionTsCache cache = new CollectionTsCache();
        cache.set("host:19530", "db", "one", 1L);
        cache.set("host:19530", "db", "two", 2L);
        cache.set("host:19530", "db", "three", 3L);

        assertEquals(1L, cache.get("host:19530", "db", "one"));
        assertEquals(2L, cache.get("host:19530", "db", "two"));
        assertEquals(3L, cache.get("host:19530", "db", "three"));
        assertEquals(3, cache.size());

        cache.set("host:19530", "other", "four", 4L);
        cache.invalidateDb("host:19530", "db");
        assertEquals(4L, cache.get("host:19530", "other", "four"));
        cache.clear();
        assertEquals(0, cache.size());
    }
}

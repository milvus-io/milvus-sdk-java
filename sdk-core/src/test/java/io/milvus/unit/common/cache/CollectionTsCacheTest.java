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
import io.milvus.common.utils.cache.CollectionTsCache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
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
    void ignoresZeroAndSupportsInvalidationAndMove() {
        CollectionTsCache cache = new CollectionTsCache();

        cache.set("host:19530", "db", "zero", 0L);
        assertEquals(0, cache.size());

        cache.set("host:19530", "db", "old", 100L);
        cache.set("host:19530", "db", "new", 200L);
        cache.move("host:19530", "db", "old", "db", "new");
        assertEquals(0L, cache.get("host:19530", "db", "old"));
        assertEquals(200L, cache.get("host:19530", "db", "new"));

        cache.move("host:19530", "db", "missing", "db", "new");
        assertEquals(200L, cache.get("host:19530", "db", "new"));

        cache.invalidate("host:19530", "db", "new");
        assertEquals(0, cache.size());
    }

    @Test
    void movesTimestampAcrossDatabases() {
        CollectionTsCache cache = new CollectionTsCache();

        cache.set("host:19530", "source", "old", 100L);
        cache.set("host:19530", "target", "new", 50L);
        cache.move("host:19530", "source", "old", "target", "new");

        assertEquals(0L, cache.get("host:19530", "source", "old"));
        assertEquals(100L, cache.get("host:19530", "target", "new"));
        assertEquals(1, cache.size());
    }

    @Test
    void copiesLatestTimestampToAliasWithoutRemovingCollection() {
        CollectionTsCache cache = new CollectionTsCache();

        cache.set("host:19530", "db", "collection", 100L);
        cache.set("host:19530", "db", "alias", 50L);
        cache.copy("host:19530", "db", "collection", "db", "alias");

        assertEquals(100L, cache.get("host:19530", "db", "collection"));
        assertEquals(100L, cache.get("host:19530", "db", "alias"));

        cache.set("host:19530", "db", "alias", 200L);
        cache.copy("host:19530", "db", "collection", "db", "alias");
        assertEquals(200L, cache.get("host:19530", "db", "alias"));
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

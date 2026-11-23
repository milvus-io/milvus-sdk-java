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

package io.milvus.v2.common;

import io.milvus.common.utils.cache.CollectionTsCache;
import io.milvus.common.utils.cache.SchemaCache;

/**
 * Advanced configuration entry point for the process-wide collection caches.
 *
 * <p>The SDK keeps in-process caches for collection schemas and for the per-collection session
 * timestamps used by SESSION consistency. Both are capacity-bounded (default 4096) with LRU
 * eviction. Applications working with a very large number of collections (for example ten
 * thousand) may raise the capacities so that frequently used collections are not evicted.
 *
 * <p>Evicting a session-timestamp entry makes SESSION (and null) consistency reads fall back to
 * {@code ts=1} for that collection until the next DML records a new timestamp, so size the
 * timestamp cache deliberately when relying on SESSION consistency.
 */
public final class CacheConfig {
    /**
     * Default maximum number of cached entries for both process-wide collection caches (schema cache
     * and collection timestamp cache), matching pymilvus's 4096-entry default.
     */
    public static final int DEFAULT_CAPACITY = 4096;

    private CacheConfig() {
    }

    // Serializes setCapacity so the two cache singletons always share one capacity: each per-cache
    // update holds its own lock, so without this a concurrent setCapacity with a different value
    // could interleave and leave the two caches at different capacities (mirrors the C++/Rust/C#
    // SDKs' shared capacity lock).
    private static final Object CAPACITY_LOCK = new Object();

    /**
     * Sets the capacity of both process-wide collection caches (schema cache and collection
     * timestamp cache) to the same value.
     *
     * @param capacity the new maximum number of cached entries for each cache
     * @throws IllegalArgumentException if {@code capacity} is not positive
     */
    public static void setCapacity(int capacity) {
        synchronized (CAPACITY_LOCK) {
            SchemaCache.setCapacity(capacity);
            CollectionTsCache.setCapacity(capacity);
        }
    }

    /**
     * Returns the capacity of the process-wide collection caches. The caches are kept at the same
     * capacity when configured through {@link #setCapacity(int)}.
     *
     * @return the maximum number of cached entries per cache
     */
    public static int getCapacity() {
        return SchemaCache.getCapacity();
    }
}

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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CollectionTsCache {
    private static final CollectionTsCache INSTANCE = new CollectionTsCache();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<CollectionCacheKey, Long> cache = new HashMap<>();

    public static CollectionTsCache getInstance() {
        return INSTANCE;
    }

    public long get(String endpoint, String databaseName, String collectionName) {
        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        lock.readLock().lock();
        try {
            return cache.getOrDefault(key, 0L);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns the latest timestamp for a database and collection without restricting the lookup
     * to an endpoint.
     *
     * <p>This method exists for compatibility with legacy public request-conversion helpers that
     * do not accept an endpoint. Normal client request paths should use {@link #get(String, String,
     * String)} so timestamps from different Milvus clusters remain isolated.</p>
     */
    public long getAnyEndpoint(String databaseName, String collectionName) {
        CollectionCacheKey target = CollectionCacheKey.create("", databaseName, collectionName);
        lock.readLock().lock();
        try {
            long latestTimestamp = 0L;
            for (Map.Entry<CollectionCacheKey, Long> entry : cache.entrySet()) {
                CollectionCacheKey key = entry.getKey();
                if (key.getDatabaseName().equals(target.getDatabaseName())
                        && key.getCollectionName().equals(target.getCollectionName())) {
                    latestTimestamp = Math.max(latestTimestamp, entry.getValue());
                }
            }
            return latestTimestamp;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void set(String endpoint, String databaseName, String collectionName, long timestamp) {
        if (timestamp == 0L) {
            return;
        }

        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        lock.writeLock().lock();
        try {
            cache.compute(key, (ignored, current) -> current == null || timestamp > current ? timestamp : current);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void invalidate(String endpoint, String databaseName, String collectionName) {
        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        lock.writeLock().lock();
        try {
            cache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void invalidateDb(String endpoint, String databaseName) {
        CollectionCacheKey prefix = CollectionCacheKey.create(endpoint, databaseName, "");
        lock.writeLock().lock();
        try {
            cache.keySet().removeIf(key -> key.getEndpoint().equals(prefix.getEndpoint())
                    && key.getDatabaseName().equals(prefix.getDatabaseName()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void rename(String endpoint, String databaseName, String oldCollectionName, String newCollectionName) {
        rename(endpoint, databaseName, oldCollectionName, databaseName, newCollectionName);
    }

    public void rename(String endpoint, String oldDatabaseName, String oldCollectionName,
                       String newDatabaseName, String newCollectionName) {
        CollectionCacheKey oldKey = CollectionCacheKey.create(endpoint, oldDatabaseName, oldCollectionName);
        CollectionCacheKey newKey = CollectionCacheKey.create(endpoint, newDatabaseName, newCollectionName);
        lock.writeLock().lock();
        try {
            if (oldKey.equals(newKey)) {
                return;
            }
            long latestTimestamp = Math.max(cache.getOrDefault(oldKey, 0L), cache.getOrDefault(newKey, 0L));
            cache.remove(oldKey);
            cache.remove(newKey);
            if (latestTimestamp != 0L) {
                cache.put(newKey, latestTimestamp);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}

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

/**
 * In-process cache storing the latest session timestamp per endpoint, database, and collection.
 * Timestamps are updated monotonically and can be invalidated or transferred on rename/alias.
 */


public class CollectionTsCache {
    private static final CollectionTsCache INSTANCE = new CollectionTsCache();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<CollectionCacheKey, Long> cache = new HashMap<>();

    /**
     * Returns the process-wide singleton instance of the cache.
     *
     * @return the shared {@link CollectionTsCache} instance
     */


    public static CollectionTsCache getInstance() {
        return INSTANCE;
    }

    /**
     * Returns the latest session timestamp cached for the collection, or {@code 0} if none.
     *
     * @param endpoint       the server endpoint
     * @param databaseName   the database name
     * @param collectionName the collection name
     * @return the cached timestamp, or {@code 0L} when not present
     */


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
     * Stores the timestamp for the collection, keeping the latest value seen.
     *
     * @param endpoint       the server endpoint
     * @param databaseName   the database name
     * @param collectionName the collection name
     * @param timestamp      the session timestamp to cache; values of {@code 0} are ignored
     */


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

    /**
     * Removes the cached timestamp of the given collection.
     *
     * @param endpoint       the server endpoint
     * @param databaseName   the database name
     * @param collectionName the collection name
     */


    public void invalidate(String endpoint, String databaseName, String collectionName) {
        CollectionCacheKey key = CollectionCacheKey.create(endpoint, databaseName, collectionName);
        lock.writeLock().lock();
        try {
            cache.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes all cached timestamps of the given database.
     *
     * @param endpoint     the server endpoint
     * @param databaseName the database name
     */


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

    /**
     * Moves the latest timestamp to a renamed collection and removes the source key.
     *
     * @param endpoint               the server endpoint
     * @param sourceDatabaseName     the database of the source collection
     * @param sourceCollectionName   the source collection name
     * @param targetDatabaseName     the database of the target collection
     * @param targetCollectionName   the target collection name
     */


    public void move(String endpoint, String sourceDatabaseName, String sourceCollectionName,
                     String targetDatabaseName, String targetCollectionName) {
        transfer(CollectionCacheKey.create(endpoint, sourceDatabaseName, sourceCollectionName),
                CollectionCacheKey.create(endpoint, targetDatabaseName, targetCollectionName), true);
    }

    /**
     * Copies the latest timestamp to an alias while retaining the collection key. The alias is
     * updated monotonically so a newer timestamp recorded through the alias is not overwritten.
     *
     * @param endpoint               the server endpoint
     * @param sourceDatabaseName     the database of the source collection
     * @param sourceCollectionName   the source collection name
     * @param targetDatabaseName     the database of the target collection
     * @param targetCollectionName   the target collection name
     */


    public void copy(String endpoint, String sourceDatabaseName, String sourceCollectionName,
                     String targetDatabaseName, String targetCollectionName) {
        transfer(CollectionCacheKey.create(endpoint, sourceDatabaseName, sourceCollectionName),
                CollectionCacheKey.create(endpoint, targetDatabaseName, targetCollectionName), false);
    }

    private void transfer(CollectionCacheKey sourceKey, CollectionCacheKey targetKey,
                          boolean dropSource) {
        lock.writeLock().lock();
        try {
            if (sourceKey.equals(targetKey)) {
                return;
            }
            long latestTimestamp = Math.max(
                    cache.getOrDefault(sourceKey, 0L), cache.getOrDefault(targetKey, 0L));
            if (dropSource) {
                cache.remove(sourceKey);
            }
            cache.remove(targetKey);
            if (latestTimestamp != 0L) {
                cache.put(targetKey, latestTimestamp);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes all cached timestamps.
     */


    public void clear() {
        lock.writeLock().lock();
        try {
            cache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns the number of entries currently cached.
     *
     * @return the cache size
     */


    public int size() {
        lock.readLock().lock();
        try {
            return cache.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}

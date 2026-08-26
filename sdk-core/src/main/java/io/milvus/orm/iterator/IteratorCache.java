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

package io.milvus.orm.iterator;

import io.milvus.exception.ParamException;
import io.milvus.response.QueryResultsWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.milvus.param.Constant.NO_CACHE_ID;

/**
 * Thread-safe cache of query results used by the query and search iterators to paginate data.
 *
 * <p>Results are stored under an integer cache ID and can be appended, drained and released
 * incrementally. The cache supports an internal read offset so that already consumed records are
 * not returned again. All operations are synchronized, making the cache safe to be shared across
 * threads.
 */
public class IteratorCache {
    private final AtomicInteger cacheId = new AtomicInteger(0);
    private final Map<Integer, CacheEntry> cacheMap = new ConcurrentHashMap<>();

    /**
     * Caches the given result rows under the specified cache ID and returns the effective ID.
     *
     * <p>If {@code cacheId} equals {@link io.milvus.param.Constant#NO_CACHE_ID}, a new ID is
     * allocated automatically.
     *
     * @param cacheId the cache ID to store under, or {@code NO_CACHE_ID} to allocate a new one
     * @param result  the result rows to cache
     * @return the cache ID under which the rows were stored
     * @throws io.milvus.exception.ParamException if {@code result} is {@code null}
     */
    public synchronized int cache(int cacheId, List<QueryResultsWrapper.RowRecord> result) {
        if (result == null) {
            throw new ParamException("Cannot cache a null result");
        }
        if (cacheId == NO_CACHE_ID) {
            cacheId = this.cacheId.incrementAndGet();
        }
        cacheMap.put(cacheId, new CacheEntry(result));
        return cacheId;
    }

    /**
     * Returns a snapshot of the unconsumed rows cached under the given cache ID.
     *
     * <p>The snapshot is a copy of the cached rows, so consuming the returned list does not change
     * the cached state.
     *
     * @param cacheId the cache ID to fetch
     * @return a copy of the cached rows, or {@code null} if the cache ID is not present
     */
    public synchronized List<QueryResultsWrapper.RowRecord> fetchCache(int cacheId) {
        CacheEntry cached = cacheMap.get(cacheId);
        return cached == null ? null : cached.snapshot();
    }

    /**
     * Returns the number of unconsumed rows cached under the given cache ID.
     *
     * @param cacheId the cache ID to check
     * @return the number of cached rows, or {@code 0} if the cache ID is not present
     */
    public synchronized int size(int cacheId) {
        CacheEntry cached = cacheMap.get(cacheId);
        return cached == null ? 0 : cached.size();
    }

    /**
     * Appends the given result rows to the cache entry under the given cache ID.
     *
     * @param cacheId the cache ID to append to
     * @param result  the result rows to append
     * @return the total number of unconsumed rows after appending
     * @throws io.milvus.exception.ParamException if {@code result} is {@code null} or
     *         {@code cacheId} is invalid
     */
    public synchronized int append(int cacheId, List<QueryResultsWrapper.RowRecord> result) {
        if (result == null) {
            throw new ParamException("Cannot append a null result to cache");
        }
        if (cacheId == NO_CACHE_ID) {
            throw new ParamException("Cannot append to an invalid cache ID");
        }

        CacheEntry cached = cacheMap.computeIfAbsent(cacheId, ignored -> new CacheEntry());
        cached.append(result);
        return cached.size();
    }

    /**
     * Drains up to the given number of rows from the cache, marking them as consumed.
     *
     * <p>Once all rows of a cache entry are drained, the entry is removed from the cache.
     *
     * @param cacheId the cache ID to drain from
     * @param count   the number of rows to drain
     * @return the drained rows
     * @throws io.milvus.exception.ParamException if {@code count} is negative, the cache ID is not
     *         present, or {@code count} exceeds the number of cached rows
     */
    public synchronized List<QueryResultsWrapper.RowRecord> drain(int cacheId, int count) {
        if (count == 0) {
            return new ArrayList<>();
        }

        CacheEntry cached = cacheMap.get(cacheId);
        if (count < 0 || cached == null || count > cached.size()) {
            String msg = String.format("Cannot drain %s results from cache %s with size %s",
                    count, cacheId, cached == null ? 0 : cached.size());
            throw new ParamException(msg);
        }

        List<QueryResultsWrapper.RowRecord> result = cached.drain(count);
        if (cached.size() == 0) {
            cacheMap.remove(cacheId);
        }
        return result;
    }

    /**
     * Removes the cache entry with the given cache ID, releasing the cached rows.
     *
     * @param cacheId the cache ID to release
     */
    public synchronized void releaseCache(int cacheId) {
        cacheMap.remove(cacheId);
    }

    private static final class CacheEntry {
        private List<QueryResultsWrapper.RowRecord> rows;
        private int offset;

        private CacheEntry() {
            this.rows = new ArrayList<>();
        }

        private CacheEntry(List<QueryResultsWrapper.RowRecord> rows) {
            this.rows = new ArrayList<>(rows);
        }

        private int size() {
            return rows.size() - offset;
        }

        private void append(List<QueryResultsWrapper.RowRecord> result) {
            rows.addAll(result);
        }

        private List<QueryResultsWrapper.RowRecord> snapshot() {
            return new ArrayList<>(rows.subList(offset, rows.size()));
        }

        private List<QueryResultsWrapper.RowRecord> drain(int count) {
            List<QueryResultsWrapper.RowRecord> result = new ArrayList<>(count);
            int end = offset + count;
            for (int i = offset; i < end; i++) {
                result.add(rows.get(i));
                rows.set(i, null);
            }
            offset = end;

            if (offset > 0 && offset >= rows.size() - offset) {
                rows = new ArrayList<>(rows.subList(offset, rows.size()));
                offset = 0;
            }
            return result;
        }
    }
}

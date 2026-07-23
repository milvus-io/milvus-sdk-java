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

public class IteratorCache {
    private final AtomicInteger cacheId = new AtomicInteger(0);
    private final Map<Integer, CacheEntry> cacheMap = new ConcurrentHashMap<>();

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

    public synchronized List<QueryResultsWrapper.RowRecord> fetchCache(int cacheId) {
        CacheEntry cached = cacheMap.get(cacheId);
        return cached == null ? null : cached.snapshot();
    }

    public synchronized int size(int cacheId) {
        CacheEntry cached = cacheMap.get(cacheId);
        return cached == null ? 0 : cached.size();
    }

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

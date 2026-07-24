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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.milvus.param.Constant.NO_CACHE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IteratorCacheTest {
    @Test
    void cacheMakesDefensiveCopyOfInput() {
        IteratorCache cache = new IteratorCache();
        List<QueryResultsWrapper.RowRecord> input = new ArrayList<>(Arrays.asList(record(1), record(2)));

        int cacheId = cache.cache(NO_CACHE_ID, input);
        input.clear();

        assertEquals(Arrays.asList(1, 2), ids(cache.fetchCache(cacheId)));
    }

    @Test
    void drainReturnsIndependentPrefixAndRetainsOnlySuffix() {
        IteratorCache cache = new IteratorCache();
        int cacheId = cache.cache(NO_CACHE_ID,
                Arrays.asList(record(1), record(2), record(3), record(4)));

        List<QueryResultsWrapper.RowRecord> drained = cache.drain(cacheId, 2);
        drained.clear();

        assertEquals(Arrays.asList(3, 4), ids(cache.fetchCache(cacheId)));
        assertEquals(2, cache.size(cacheId));
    }

    @Test
    void fullDrainReleasesCacheEntry() {
        IteratorCache cache = new IteratorCache();
        int cacheId = cache.cache(NO_CACHE_ID, Arrays.asList(record(1), record(2)));

        assertEquals(Arrays.asList(1, 2), ids(cache.drain(cacheId, 2)));
        assertEquals(0, cache.size(cacheId));
        assertNull(cache.fetchCache(cacheId));
    }

    @Test
    void repeatedAppendAndDrainKeepsOnlyUnconsumedRows() {
        IteratorCache cache = new IteratorCache();
        int cacheId = cache.cache(NO_CACHE_ID, Arrays.asList(record(1), record(2), record(3)));

        assertEquals(Arrays.asList(1, 2), ids(cache.drain(cacheId, 2)));
        assertEquals(3, cache.append(cacheId, Arrays.asList(record(4), record(5))));
        assertEquals(Arrays.asList(3, 4), ids(cache.drain(cacheId, 2)));
        assertEquals(Arrays.asList(5), ids(cache.fetchCache(cacheId)));

        cache.drain(cacheId, 1);
        assertEquals(2, cache.append(cacheId, Arrays.asList(record(6), record(7))));
        assertEquals(Arrays.asList(6, 7), ids(cache.fetchCache(cacheId)));
    }

    @Test
    void repeatedSmallDrainsPreserveOrderAcrossCompaction() {
        IteratorCache cache = new IteratorCache();
        int cacheId = cache.cache(NO_CACHE_ID, Arrays.asList(
                record(1), record(2), record(3), record(4),
                record(5), record(6), record(7), record(8)));

        for (int expected = 1; expected <= 6; expected++) {
            assertEquals(Arrays.asList(expected), ids(cache.drain(cacheId, 1)));
        }
        cache.append(cacheId, Arrays.asList(record(9), record(10)));

        assertEquals(Arrays.asList(7, 8, 9, 10), ids(cache.fetchCache(cacheId)));
    }

    @Test
    void drainRejectsInvalidRequests() {
        IteratorCache cache = new IteratorCache();
        int cacheId = cache.cache(NO_CACHE_ID, Arrays.asList(record(1), record(2)));

        assertThrows(ParamException.class, () -> cache.drain(cacheId, -1));
        assertThrows(ParamException.class, () -> cache.drain(cacheId, 3));
        assertThrows(ParamException.class, () -> cache.drain(cacheId + 1, 1));
    }

    @Test
    void zeroCountDrainIsNoOpWithOrWithoutCache() {
        IteratorCache cache = new IteratorCache();

        assertTrue(cache.drain(NO_CACHE_ID, 0).isEmpty());

        int cacheId = cache.cache(NO_CACHE_ID, Arrays.asList(record(1), record(2)));
        assertTrue(cache.drain(cacheId, 0).isEmpty());
        assertEquals(Arrays.asList(1, 2), ids(cache.fetchCache(cacheId)));
    }

    private static QueryResultsWrapper.RowRecord record(int id) {
        QueryResultsWrapper.RowRecord record = new QueryResultsWrapper.RowRecord();
        record.put("id", id);
        return record;
    }

    private static List<Integer> ids(List<QueryResultsWrapper.RowRecord> records) {
        List<Integer> ids = new ArrayList<>();
        for (QueryResultsWrapper.RowRecord record : records) {
            ids.add((Integer) record.get("id"));
        }
        return ids;
    }
}

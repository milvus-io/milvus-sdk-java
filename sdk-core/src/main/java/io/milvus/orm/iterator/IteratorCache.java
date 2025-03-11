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

import io.milvus.response.QueryResultsWrapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.milvus.param.Constant.NO_CACHE_ID;

public class IteratorCache {
    private final AtomicInteger cacheId = new AtomicInteger(0);
    private final Map<Integer, List<QueryResultsWrapper.RowRecord>> cacheMap = new ConcurrentHashMap<>();

    public int cache(int cacheId, List<QueryResultsWrapper.RowRecord> result) {
        if (cacheId == NO_CACHE_ID) {
            cacheId = this.cacheId.incrementAndGet();
        }
        cacheMap.put(cacheId, result);
        return cacheId;
    }

    public List<QueryResultsWrapper.RowRecord> fetchCache(int cacheId) {
        return cacheMap.getOrDefault(cacheId, null);
    }

    public void releaseCache(int cacheId) {
        cacheMap.remove(cacheId);
    }
}
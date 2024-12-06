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
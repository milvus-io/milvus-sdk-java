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

import io.milvus.common.utils.ExceptionUtils;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.SearchResp;
import io.milvus.v2.utils.ConvertUtils;
import io.milvus.v2.utils.RpcUtils;
import io.milvus.v2.utils.VectorUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.milvus.param.Constant.MAX_BATCH_SIZE;
import static io.milvus.param.Constant.UNLIMITED;

/**
 * Server-side iterator for paginating vector search results using the Search Iterator V2 protocol.
 *
 * <p>Unlike the v1 search iterator, pagination is driven by a server-issued token: the first probe
 * returns a token and the iterator passes it back in subsequent calls together with the last-bound
 * distance to fetch the next page. The iterator requires a Milvus server of version 2.5.2 or later.
 *
 * <p>Results can be post-filtered by an external filter function; in that case the iterator keeps
 * probing until a batch that satisfies the filter is collected. The configured limit caps the total
 * number of returned rows, and {@link #next()} returns an empty list once the limit is reached.
 */
public class SearchIteratorV2 {
    private static final Logger logger = LoggerFactory.getLogger(SearchIteratorV2.class);
    private final RpcStubWrapper blockingStub;

    private final SearchIteratorReqV2 searchIteratorReq;
    private final int batchSize;

    private Map<String, Object> searchParams;
    private final RpcUtils rpcUtils;
    private final VectorUtils vectorUtils;
    private final String clusterId;

    private Long leftResCnt = null;
    private Long collectionID = null;
    private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = null;
    private List<SearchResp.SearchResult> cache = new ArrayList<>();

    /**
     * Creates a Search Iterator V2 from the given request.
     *
     * <p>The iterator is probed for server compatibility during construction.
     *
     * @param searchIteratorReq the search iterator request
     * @param blockingStub      the gRPC stub wrapper used to perform searches
     */
    public SearchIteratorV2(SearchIteratorReqV2 searchIteratorReq,
                            RpcStubWrapper blockingStub) {
        this(searchIteratorReq, blockingStub, null);
    }

    /**
     * Creates a Search Iterator V2 from the given request.
     *
     * <p>The iterator is probed for server compatibility during construction.
     *
     * @param searchIteratorReq the search iterator request
     * @param blockingStub      the gRPC stub wrapper used to perform searches
     * @param clusterId         the cluster ID for global cluster routing, may be empty
     */
    public SearchIteratorV2(SearchIteratorReqV2 searchIteratorReq,
                            RpcStubWrapper blockingStub,
                            String clusterId) {
        this.blockingStub = blockingStub;
        this.searchIteratorReq = searchIteratorReq;
        this.clusterId = clusterId;

        this.batchSize = (int) searchIteratorReq.getBatchSize();
        this.externalFilterFunc = searchIteratorReq.getExternalFilterFunc();
        this.rpcUtils = new RpcUtils();
        this.vectorUtils = new VectorUtils();
        this.vectorUtils.setEndpoint(blockingStub.getEndpoint());
        this.vectorUtils.setCurrentDbName(blockingStub.getDatabaseName());

        checkParams();
        setupCollectionID();
        probeForCompability();
    }

    private void checkParams() {
        if (this.batchSize < 0) {
            ExceptionUtils.throwUnExpectedException("Batch size cannot be less than zero");
        } else if (this.batchSize > MAX_BATCH_SIZE) {
            ExceptionUtils.throwUnExpectedException(String.format("Batch size cannot be larger than %d", MAX_BATCH_SIZE));
        }

        searchParams = searchIteratorReq.getSearchParams();
        if (searchParams.containsKey(Constant.OFFSET) && (int) searchParams.get(Constant.OFFSET) > 0) {
            ExceptionUtils.throwUnExpectedException("Offset is not supported for SearchIterator");
        }

        int rows = searchIteratorReq.getVectors().size();
        if (rows > 1) {
            ExceptionUtils.throwUnExpectedException("SearchIterator does not support processing multiple vectors simultaneously");
        } else if (rows == 0) {
            ExceptionUtils.throwUnExpectedException("The vector data for search cannot be empty");
        }

        if (searchIteratorReq.getLimit() != UNLIMITED) {
            this.leftResCnt = searchIteratorReq.getLimit();
        }
    }

    private void setupCollectionID() {
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(searchIteratorReq.getCollectionName());
        if (StringUtils.isNotEmpty(searchIteratorReq.getDatabaseName())) {
            builder.setDbName(searchIteratorReq.getDatabaseName());
        }
        DescribeCollectionResponse response = rpcUtils.retry(() -> blockingStub.get().describeCollection(builder.build()));
        String title = String.format("DescribeCollectionRequest collectionName:%s", searchIteratorReq.getCollectionName());
        rpcUtils.handleResponse(title, response.getStatus());

        DescribeCollectionResp respR = new ConvertUtils().convertDescCollectionResp(response);
        this.collectionID = respR.getCollectionID();
    }

    private SearchResults executeSearch(int limit) {
        searchParams.put("search_iter_batch_size", limit);
        SearchReq request = SearchReq.builder()
                .collectionName(searchIteratorReq.getCollectionName())
                .partitionNames(searchIteratorReq.getPartitionNames())
                .databaseName(searchIteratorReq.getDatabaseName())
                .annsField(searchIteratorReq.getVectorFieldName())
                .data(searchIteratorReq.getVectors())
                .limit(limit)
                .filter(searchIteratorReq.getFilter())
                .consistencyLevel(searchIteratorReq.getConsistencyLevel())
                .outputFields(searchIteratorReq.getOutputFields())
                .roundDecimal(searchIteratorReq.getRoundDecimal())
                .searchParams(searchParams)
                .metricType(searchIteratorReq.getMetricType())
                .timezone(searchIteratorReq.getTimezone())
                .ignoreGrowing(searchIteratorReq.isIgnoreGrowing())
                .groupByFieldName(searchIteratorReq.getGroupByFieldName())
                .filterTemplateValues(searchIteratorReq.getFilterTemplateValues())
                .build();
        SearchRequest searchRequest = vectorUtils.ConvertToGrpcSearchRequest(request);
        SearchRequest.Builder builder = searchRequest.toBuilder();
        if (StringUtils.isNotEmpty(clusterId)) {
            builder.addSearchParams(KeyValuePair.newBuilder()
                    .setKey(Constant.CLUSTER_ID)
                    .setValue(clusterId)
                    .build());
        }
        SearchResults response = rpcUtils.retry(() -> blockingStub.get().search(builder.build()));
        String title = String.format("SearchRequest collectionName:%s", searchIteratorReq.getCollectionName());
        rpcUtils.handleResponse(title, response.getStatus());

        return response;
    }

    private void probeForCompability() {
        searchParams.put("collection_id", this.collectionID);
        searchParams.put("iterator", true);
        searchParams.put("search_iter_v2", true);
        searchParams.putIfAbsent("guarantee_timestamp", 0L);

        SearchResults response = executeSearch(1);
        checkTokenExists(response.getResults());
        setupGuaranteeTimestamp(response);
    }

    private void checkTokenExists(SearchResultData resultData) {
        String token = resultData.getSearchIteratorV2Results().getToken();
        if (StringUtils.isEmpty(token)) {
            ExceptionUtils.throwUnExpectedException("The server does not support Search Iterator V2." +
                    " The search_iterator (v1) is used instead.\n" +
                    "    Please upgrade your Milvus server version to 2.5.2 and later,\n" +
                    "    or use a pymilvus version before 2.5.3 (excluded) to avoid this issue.");
        }
    }

    /**
     * Returns the next batch of search results.
     *
     * <p>The returned list contains up to {@code batchSize} rows. When an external filter function
     * is configured, the iterator continues fetching pages until the batch satisfies the filter or
     * the server has no more results. Once the configured limit is reached, an empty list is
     * returned.
     *
     * @return the next batch of rows, or an empty list if the iteration is finished
     */
    public List<SearchResp.SearchResult> next() {
        if (leftResCnt != null && leftResCnt <= 0) {
            cache.clear();
            return new ArrayList<>();
        }

        if (externalFilterFunc == null) {
            return wrapReturnRes(_next());
        }

        int targetLen = batchSize;
        if (leftResCnt != null && leftResCnt < targetLen) {
            targetLen = leftResCnt.intValue();
        }

        while (true) {
            List<SearchResp.SearchResult> hits = _next();
            if (hits == null || hits.isEmpty()) {
                break;
            }

            if (externalFilterFunc != null) {
                hits = externalFilterFunc.apply(hits);
            }

            cache.addAll(hits);
            if (cache.size() >= targetLen) {
                break;
            }
        }

        // create a list with elements from 0 to targetLen, and remove the elements from cache
        targetLen = Math.min(cache.size(), targetLen);
        List<SearchResp.SearchResult> subList = cache.subList(0, targetLen);
        List<SearchResp.SearchResult> ret = new ArrayList<>(subList);
        subList.clear();
        return wrapReturnRes(ret);
    }

    private List<SearchResp.SearchResult> _next() {
        SearchResults response = executeSearch(batchSize);
        checkTokenExists(response.getResults());
        SearchIteratorV2Results iterInfo = response.getResults().getSearchIteratorV2Results();
        searchParams.put("search_iter_last_bound", iterInfo.getLastBound());

        if (!searchParams.containsKey("search_iter_id")) {
            searchParams.put("search_iter_id", iterInfo.getToken());
        }

        setupGuaranteeTimestamp(response);

        List<List<SearchResp.SearchResult>> res = new ConvertUtils().getEntities(response);
        return res.get(0);
    }

    private void setupGuaranteeTimestamp(SearchResults response) {
        long ts = ((Number) searchParams.get("guarantee_timestamp")).longValue();
        if (ts <= 0) {
            if (response.getSessionTs() > 0) {
                searchParams.put("guarantee_timestamp", response.getSessionTs());
            } else {
                logger.warn("Failed to set up mvccTs from milvus server, use client-side ts instead");

                long clientTs = System.currentTimeMillis() + 1000L;
                clientTs = clientTs << 18;
                searchParams.put("guarantee_timestamp", clientTs);
            }
        }
    }

    private List<SearchResp.SearchResult> wrapReturnRes(List<SearchResp.SearchResult> res) {
        if (leftResCnt == null) {
            return res;
        }

        int currentLen = res.size();
        if (currentLen > leftResCnt) {
            res = new ArrayList<>(res.subList(0, leftResCnt.intValue()));
        }
        leftResCnt = Math.max(0L, leftResCnt - res.size());
        if (leftResCnt == 0) {
            cache.clear();
        }
        return res;
    }

    /**
     * Clears the internal cache of buffered search results.
     */
    public void close() {
        cache.clear();
    }
}

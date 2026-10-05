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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static io.milvus.param.Constant.MAX_BATCH_SIZE;
import static io.milvus.param.Constant.UNLIMITED;

/**
 * Server-side iterator for paginating vector search results using the Search Iterator V2 protocol.
 *
 * <p>Unlike the v1 search iterator, pagination is driven by a server-issued token: the first real page
 * returns a token and the iterator passes it back in subsequent calls together with the last-bound
 * distance to fetch the next page. The iterator requires a Milvus server of version 2.5.2 or later.
 *
 * <p>Results can be post-filtered by an external filter function; in that case the iterator keeps
 * fetching until a batch that satisfies the filter is collected. The configured limit caps the total
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
    private DataType primaryKeyType;
    private String cursorVersion;
    private boolean manualLegacyCursor;
    private boolean requestPkCursor;
    private SearchResults pendingResponse;
    private Map<String, Object> pendingUpdates;
    private boolean exhausted = false;
    private static final String CURSOR_VERSION = "search_iter_cursor_version";
    private static final String LAST_PK_TYPE = "search_iter_last_pk_type";
    private static final String LAST_PK = "search_iter_last_pk";
    private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = null;
    private List<SearchResp.SearchResult> cache = new ArrayList<>();
    private final java.util.Set<Object> acceptedPrimaryKeys = new java.util.HashSet<>();

    /**
     * Creates a Search Iterator V2 from the given request.
     *
     * <p>The first real page detects server compatibility and is buffered during construction.
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
     * <p>The first real page detects server compatibility and is buffered during construction.
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
        initializeFirstPage();
    }

    private void checkParams() {
        if (searchIteratorReq.getBatchSize() <= 0) {
            ExceptionUtils.throwUnExpectedException("Batch size must be greater than zero");
        } else if (searchIteratorReq.getBatchSize() > MAX_BATCH_SIZE) {
            ExceptionUtils.throwUnExpectedException(String.format("Batch size cannot be larger than %d", MAX_BATCH_SIZE));
        }

        searchParams = new HashMap<>(searchIteratorReq.getSearchParams());
        Object requestedVersion = searchParams.get(CURSOR_VERSION);
        if (requestedVersion != null && !String.valueOf(requestedVersion).isEmpty() && !"2".equals(String.valueOf(requestedVersion))) {
            throw new IllegalArgumentException("Unsupported search iterator cursor version");
        }
        requestPkCursor = requestedVersion != null && "2".equals(String.valueOf(requestedVersion));
        Object originalToken = searchParams.get("search_iter_id");
        manualLegacyCursor = searchParams.get("search_iter_last_bound") != null ||
                (originalToken != null && !String.valueOf(originalToken).isEmpty());
        if (manualLegacyCursor && requestPkCursor) {
            throw new IllegalArgumentException("A legacy search iterator continuation cannot opt into a PK cursor");
        }
        for (String key : new String[]{CURSOR_VERSION, LAST_PK_TYPE, LAST_PK}) {
            searchParams.remove(key);
        }
        if (!manualLegacyCursor) {
            searchParams.remove("search_iter_id");
            searchParams.remove("search_iter_last_bound");
        }
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
        for (FieldSchema field : response.getSchema().getFieldsList()) {
            if (field.getIsPrimaryKey()) {
                primaryKeyType = field.getDataType();
                break;
            }
        }
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
                .searchParams(new HashMap<>(searchParams))
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

    private void initializeFirstPage() {
        searchParams.put("collection_id", this.collectionID);
        searchParams.put("iterator", true);
        searchParams.put("search_iter_v2", true);
        searchParams.putIfAbsent("guarantee_timestamp", 0L);
        if (requestPkCursor) {
            searchParams.put(CURSOR_VERSION, "2");
        }
        SearchResults response = executeSearch(batchSize);
        Map<String, Object> updates = validatePage(response, true);
        cursorVersion = response.getStatus().getExtraInfoMap().get(CURSOR_VERSION);
        if (cursorVersion == null) {
            searchParams.remove(CURSOR_VERSION);
        }
        searchParams.put("guarantee_timestamp", updates.get("guarantee_timestamp"));
        pendingResponse = response;
        pendingUpdates = updates;
    }

    private void checkTokenExists(SearchResultData resultData) {
        String token = resultData.getSearchIteratorV2Results().getToken();
        if (StringUtils.isEmpty(token)) {
            ExceptionUtils.throwUnExpectedException("The server does not support Search Iterator V2." +
                    " Use the V1 search iterator for this server.\n" +
                    "    Please upgrade your Milvus server version to 2.5.2 and later,\n" +
                    "    or use the legacy search iterator API.");
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
        int targetLen = batchSize;
        if (leftResCnt != null && leftResCnt < targetLen) {
            targetLen = leftResCnt.intValue();
        }
        while (cache.size() < targetLen && !exhausted) {
            if (pendingResponse == null) {
                SearchResults response = executeSearch(batchSize);
                Map<String, Object> updates = validatePage(response, false);
                pendingResponse = response;
                pendingUpdates = updates;
            }
            // Decode anew from the immutable proto, so a mutating callback that
            // throws can retry the same pending page without advancing its cursor.
            List<SearchResp.SearchResult> hits = new ConvertUtils().getEntities(pendingResponse).get(0);
            boolean empty = hits.isEmpty();
            if (!empty && externalFilterFunc != null) {
                hits = externalFilterFunc.apply(hits);
                if (hits == null) {
                    throw new IllegalStateException("Search iterator external filter returned null");
                }
            }
            if ("2".equals(cursorVersion)) {
                java.util.Set<Object> pageKeys = new java.util.HashSet<>();
                List<SearchResp.SearchResult> distinct = new ArrayList<>();
                for (SearchResp.SearchResult hit : hits) {
                    Object key = hit.getId();
                    if (!acceptedPrimaryKeys.contains(key) && pageKeys.add(key)) {
                        distinct.add(hit);
                    }
                }
                cache.addAll(distinct);
                acceptedPrimaryKeys.addAll(pageKeys);
            } else {
                cache.addAll(hits);
            }
            searchParams.putAll(pendingUpdates);
            pendingResponse = null;
            pendingUpdates = null;
            exhausted = empty;
            if (externalFilterFunc == null && (!"2".equals(cursorVersion) || !cache.isEmpty())) {
                break;
            }
        }
        targetLen = Math.min(cache.size(), targetLen);
        List<SearchResp.SearchResult> ret = new ArrayList<>(cache.subList(0, targetLen));
        cache.subList(0, targetLen).clear();
        return wrapReturnRes(ret);
    }

    private Map<String, Object> validatePage(SearchResults response, boolean initial) {
        SearchResultData data = response.getResults();
        SearchIteratorV2Results info = data.getSearchIteratorV2Results();
        Map<String, String> metadata = response.getStatus().getExtraInfoMap();
        String version = metadata.get(CURSOR_VERSION);
        if (initial && !requestPkCursor && version != null) {
            throw new IllegalStateException("Search iterator returned a cursor version that was not requested");
        }
        if (initial && version == null) {
            checkTokenExists(data);
        } else if (info.getToken().isEmpty()) {
            throw new IllegalStateException("Search iterator response is missing its token");
        }
        if (version != null && !"2".equals(version)) {
            throw new IllegalStateException("Unsupported search iterator cursor version");
        }
        if (!initial && !java.util.Objects.equals(version, cursorVersion)) {
            throw new IllegalStateException("Search iterator cursor version changed during iteration");
        }
        if ("2".equals(version) && searchParams.containsKey("search_iter_id") &&
                !info.getToken().equals(searchParams.get("search_iter_id"))) {
            throw new IllegalStateException("Search iterator token changed during iteration");
        }
        if ("2".equals(version)) {
            long count = data.getTopksCount() == 1 ? data.getTopks(0) : -1;
            int idCount = data.getIds().hasIntId() ? data.getIds().getIntId().getDataCount() :
                    data.getIds().hasStrId() ? data.getIds().getStrId().getDataCount() : 0;
            if (data.getNumQueries() != 1 || count < 0 || count != idCount || count != data.getScoresCount() ||
                    (count > 0 && info.getLastBound() != data.getScores(data.getScoresCount() - 1))) {
                throw new IllegalStateException("Search iterator cursor does not match the raw result shape or score");
            }
            for (float score : data.getScoresList()) {
                if (!Float.isFinite(score)) {
                    throw new IllegalStateException("Search iterator returned a non-finite raw score");
                }
            }
        }
        // Conversion and shape validation happen before any state is committed.
        List<List<SearchResp.SearchResult>> decoded = new ConvertUtils().getEntities(response);
        if (decoded.size() != 1) {
            throw new IllegalStateException("Search iterator response must contain exactly one query");
        }
        Map<String, Object> updates = new HashMap<>();
        long timestamp = ((Number) searchParams.get("guarantee_timestamp")).longValue();
        if (timestamp <= 0) {
            timestamp = response.getSessionTs();
            if (timestamp <= 0) {
                if ("2".equals(version)) {
                    throw new IllegalStateException("PK search iterator requires a positive snapshot timestamp");
                }
                logger.warn("Failed to set up mvccTs from milvus server, use client-side ts instead");
                timestamp = (System.currentTimeMillis() + 1000L) << 18;
            }
        }
        updates.put("guarantee_timestamp", timestamp);
        if (!decoded.get(0).isEmpty()) {
            if (!Float.isFinite(info.getLastBound())) {
                throw new IllegalStateException("Search iterator returned a non-finite distance bound");
            }
            updates.put("search_iter_last_bound", info.getLastBound());
            if ("2".equals(version)) {
                String type = metadata.get(LAST_PK_TYPE);
                String value = metadata.get(LAST_PK);
                if (primaryKeyType == DataType.Int64 && "int64".equals(type) && value != null &&
                        value.matches("-?(0|[1-9][0-9]*)") && data.getIds().hasIntId()) {
                    long pk;
                    try {
                        pk = Long.parseLong(value);
                    } catch (NumberFormatException error) {
                        throw new IllegalStateException("Invalid int64 search iterator PK cursor", error);
                    }
                    List<Long> ids = data.getIds().getIntId().getDataList();
                    if (ids.isEmpty() || pk != ids.get(ids.size() - 1)) {
                        throw new IllegalStateException("Search iterator PK cursor does not match the response IDs");
                    }
                } else if (primaryKeyType == DataType.VarChar && "varchar".equals(type) && value != null &&
                        data.getIds().hasStrId()) {
                    List<String> ids = data.getIds().getStrId().getDataList();
                    if (ids.isEmpty() || !value.equals(ids.get(ids.size() - 1))) {
                        throw new IllegalStateException("Search iterator PK cursor does not match the response IDs");
                    }
                } else {
                    throw new IllegalStateException("Search iterator PK cursor does not match the collection schema");
                }
                updates.put(LAST_PK_TYPE, type);
                updates.put(LAST_PK, value);
            }
        }
        if (!searchParams.containsKey("search_iter_id")) {
            updates.put("search_iter_id", info.getToken());
        }
        return updates;
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

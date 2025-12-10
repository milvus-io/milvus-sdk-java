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

public class SearchIteratorV2 {
    private static final Logger logger = LoggerFactory.getLogger(SearchIterator.class);
    private final RpcStubWrapper blockingStub;

    private final SearchIteratorReqV2 searchIteratorReq;
    private final int batchSize;

    private Map<String, Object> searchParams;
    private final RpcUtils rpcUtils;

    private Integer leftResCnt = null;
    private Long collectionID = null;
    private Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = null;
    private List<SearchResp.SearchResult> cache = new ArrayList<>();

    // to support V2
    public SearchIteratorV2(SearchIteratorReqV2 searchIteratorReq,
                            RpcStubWrapper blockingStub) {
        this.blockingStub = blockingStub;
        this.searchIteratorReq = searchIteratorReq;

        this.batchSize = (int) searchIteratorReq.getBatchSize();
        this.externalFilterFunc = searchIteratorReq.getExternalFilterFunc();
        this.rpcUtils = new RpcUtils();

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
        } else if (rows <= 0) {
            ExceptionUtils.throwUnExpectedException("The vector data for search cannot be empty");
        }

        if (searchIteratorReq.getTopK() != UNLIMITED) {
            this.leftResCnt = searchIteratorReq.getTopK();
        }
    }

    private void setupCollectionID() {
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(searchIteratorReq.getCollectionName());
        if (StringUtils.isNotEmpty(searchIteratorReq.getDatabaseName())) {
            builder.setDbName(searchIteratorReq.getDatabaseName());
        }
        DescribeCollectionResponse response = rpcUtils.retry(() -> this.blockingStub.get().describeCollection(builder.build()));
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
                .topK(limit)
                .filter(searchIteratorReq.getFilter())
                .consistencyLevel(searchIteratorReq.getConsistencyLevel())
                .outputFields(searchIteratorReq.getOutputFields())
                .roundDecimal(searchIteratorReq.getRoundDecimal())
                .searchParams(searchParams)
                .metricType(searchIteratorReq.getMetricType())
                .ignoreGrowing(searchIteratorReq.isIgnoreGrowing())
                .groupByFieldName(searchIteratorReq.getGroupByFieldName())
                .build();
        SearchRequest searchRequest = new VectorUtils().ConvertToGrpcSearchRequest(request);
        SearchResults response = rpcUtils.retry(() -> this.blockingStub.get().search(searchRequest));
        String title = String.format("SearchRequest collectionName:%s", searchIteratorReq.getCollectionName());
        rpcUtils.handleResponse(title, response.getStatus());

        return response;
    }

    private void probeForCompability() {
        searchParams.put("collection_id", this.collectionID);
        searchParams.put("iterator", true);
        searchParams.put("search_iter_v2", true);
        searchParams.put("guarantee_timestamp", 0L);

        SearchResultData resultData = executeSearch(1).getResults();
        checkTokenExists(resultData);
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

    public List<SearchResp.SearchResult> next() {
        if (leftResCnt != null && leftResCnt <= 0) {
            return new ArrayList<>();
        }

        if (externalFilterFunc == null) {
            return wrapReturnRes(_next());
        }

        int targetLen = batchSize;
        if (leftResCnt != null && leftResCnt < targetLen) {
            targetLen = leftResCnt;
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

        long ts = (long) searchParams.get("guarantee_timestamp");
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

        List<List<SearchResp.SearchResult>> res = new ConvertUtils().getEntities(response);
        return res.get(0);
    }

    private List<SearchResp.SearchResult> wrapReturnRes(List<SearchResp.SearchResult> res) {
        if (leftResCnt == null) {
            return res;
        }

        int currentLen = res.size();
        if (currentLen > leftResCnt) {
            res = res.subList(0, leftResCnt);
        }
        leftResCnt -= currentLen;
        return res;
    }

    public void close() {
    }
}

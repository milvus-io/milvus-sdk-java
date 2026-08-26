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

package io.milvus.v2.client;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.concurrent.CompletableFuture;

/**
 * A session scoped to a specific cluster of a global cluster deployment.
 * <p>
 * All DML and DQL operations issued through a session are routed to the bound cluster. The session
 * becomes unusable once {@link #close()} is called.
 */
public class MilvusClientV2Session {
    private final MilvusClientV2 parent;
    private final String clusterId;
    private boolean closed = false;

    MilvusClientV2Session(MilvusClientV2 parent, String clusterId) {
        this.parent = parent;
        this.clusterId = clusterId;
    }

    /**
     * Returns the cluster ID that this session is bound to.
     *
     * @return the cluster ID
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Searches vectors in the bound cluster. See {@link MilvusClientV2#search(SearchReq)}.
     *
     * @param request search request
     * @return {@link SearchResp}
     */
    public SearchResp search(SearchReq request) {
        ensureOpen();
        return parent.search(request, clusterId);
    }

    /**
     * Searches vectors asynchronously in the bound cluster. See {@link MilvusClientV2#searchAsync(SearchReq)}.
     *
     * @param request search request
     * @return a future completed with {@link SearchResp}, or exceptionally when the operation fails
     */
    public CompletableFuture<SearchResp> searchAsync(SearchReq request) {
        ensureOpen();
        return parent.searchAsync(request, clusterId);
    }

    /**
     * Performs a hybrid search (vector plus full-text) in the bound cluster. See {@link MilvusClientV2#hybridSearch(HybridSearchReq)}.
     *
     * @param request hybrid search request
     * @return {@link SearchResp}
     */
    public SearchResp hybridSearch(HybridSearchReq request) {
        ensureOpen();
        return parent.hybridSearch(request, clusterId);
    }

    /**
     * Performs a hybrid search asynchronously in the bound cluster. See {@link MilvusClientV2#hybridSearchAsync(HybridSearchReq)}.
     *
     * @param request hybrid search request
     * @return a future completed with {@link SearchResp}, or exceptionally when the operation fails
     */
    public CompletableFuture<SearchResp> hybridSearchAsync(HybridSearchReq request) {
        ensureOpen();
        return parent.hybridSearchAsync(request, clusterId);
    }

    /**
     * Queries data in the bound cluster. See {@link MilvusClientV2#query(QueryReq)}.
     *
     * @param request query request
     * @return {@link QueryResp}
     */
    public QueryResp query(QueryReq request) {
        ensureOpen();
        return parent.query(request, clusterId);
    }

    /**
     * Queries data asynchronously in the bound cluster. See {@link MilvusClientV2#queryAsync(QueryReq)}.
     *
     * @param request query request
     * @return a future completed with {@link QueryResp}, or exceptionally when the operation fails
     */
    public CompletableFuture<QueryResp> queryAsync(QueryReq request) {
        ensureOpen();
        return parent.queryAsync(request, clusterId);
    }

    /**
     * Creates a query iterator in the bound cluster. See {@link MilvusClientV2#queryIterator(QueryIteratorReq)}.
     *
     * @param request query iterator request
     * @return {@link QueryIterator}
     */
    public QueryIterator queryIterator(QueryIteratorReq request) {
        ensureOpen();
        return parent.queryIterator(request, clusterId);
    }

    /**
     * Creates a search iterator in the bound cluster. See {@link MilvusClientV2#searchIterator(SearchIteratorReq)}.
     *
     * @param request search iterator request
     * @return {@link SearchIterator}
     */
    public SearchIterator searchIterator(SearchIteratorReq request) {
        ensureOpen();
        return parent.searchIterator(request, clusterId);
    }

    /**
     * Creates a V2 search iterator in the bound cluster. See {@link MilvusClientV2#searchIteratorV2(SearchIteratorReqV2)}.
     *
     * @param request search iterator V2 request
     * @return {@link SearchIteratorV2}
     */
    public SearchIteratorV2 searchIteratorV2(SearchIteratorReqV2 request) {
        ensureOpen();
        return parent.searchIteratorV2(request, clusterId);
    }

    /**
     * Retrieves entities by primary key in the bound cluster. See {@link MilvusClientV2#get(GetReq)}.
     *
     * @param request get request
     * @return {@link GetResp}
     */
    public GetResp get(GetReq request) {
        ensureOpen();
        return parent.get(request, clusterId);
    }

    /**
     * Retrieves entities by primary key asynchronously in the bound cluster. See {@link MilvusClientV2#getAsync(GetReq)}.
     *
     * @param request get request
     * @return a future completed with {@link GetResp}, or exceptionally when the operation fails
     */
    public CompletableFuture<GetResp> getAsync(GetReq request) {
        ensureOpen();
        return parent.getAsync(request, clusterId);
    }

    /**
     * Closes this session. Any subsequent operation on the session fails with
     * {@link MilvusClientException}.
     */
    public void close() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "MilvusClient session is closed");
        }
    }
}

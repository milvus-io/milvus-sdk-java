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

public class MilvusClientV2Session {
    private final MilvusClientV2 parent;
    private final String clusterId;
    private boolean closed = false;

    MilvusClientV2Session(MilvusClientV2 parent, String clusterId) {
        this.parent = parent;
        this.clusterId = clusterId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public SearchResp search(SearchReq request) {
        ensureOpen();
        return parent.search(request, clusterId);
    }

    public CompletableFuture<SearchResp> searchAsync(SearchReq request) {
        ensureOpen();
        return parent.searchAsync(request, clusterId);
    }

    public SearchResp hybridSearch(HybridSearchReq request) {
        ensureOpen();
        return parent.hybridSearch(request, clusterId);
    }

    public CompletableFuture<SearchResp> hybridSearchAsync(HybridSearchReq request) {
        ensureOpen();
        return parent.hybridSearchAsync(request, clusterId);
    }

    public QueryResp query(QueryReq request) {
        ensureOpen();
        return parent.query(request, clusterId);
    }

    public CompletableFuture<QueryResp> queryAsync(QueryReq request) {
        ensureOpen();
        return parent.queryAsync(request, clusterId);
    }

    public QueryIterator queryIterator(QueryIteratorReq request) {
        ensureOpen();
        return parent.queryIterator(request, clusterId);
    }

    public SearchIterator searchIterator(SearchIteratorReq request) {
        ensureOpen();
        return parent.searchIterator(request, clusterId);
    }

    public SearchIteratorV2 searchIteratorV2(SearchIteratorReqV2 request) {
        ensureOpen();
        return parent.searchIteratorV2(request, clusterId);
    }

    public GetResp get(GetReq request) {
        ensureOpen();
        return parent.get(request, clusterId);
    }

    public CompletableFuture<GetResp> getAsync(GetReq request) {
        ensureOpen();
        return parent.getAsync(request, clusterId);
    }

    public void close() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "MilvusClient session is closed");
        }
    }
}

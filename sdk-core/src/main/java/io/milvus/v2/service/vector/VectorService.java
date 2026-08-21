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

package io.milvus.v2.service.vector;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.milvus.common.utils.JsonUtils;
import io.milvus.common.utils.cache.SchemaCache;
import io.milvus.grpc.*;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.RpcStubWrapper;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.param.Constant;
import io.milvus.v2.exception.DataNotMatchException;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.utils.DataUtils;
import io.milvus.v2.utils.RpcUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class VectorService extends BaseService {
    Logger logger = LoggerFactory.getLogger(VectorService.class);
    private DescribeCollectionResponse describeCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                          String dbName, String collectionName) {
        String title = String.format("Describe collection '%s' in database: '%s'", collectionName, dbName);
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        DescribeCollectionResponse response = blockingStub.describeCollection(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        return response;
    }

    private CompletableFuture<DescribeCollectionResponse> describeCollection(
            MilvusServiceGrpc.MilvusServiceFutureStub futureStub,
            String dbName, String collectionName) {
        String title = String.format("Describe collection '%s' in database: '%s'", collectionName, dbName);
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }
        return transformFuture(futureStub.describeCollection(builder.build()), response -> {
            rpcUtils.handleResponse(title, response.getStatus());
            return response;
        });
    }

    /**
     * Returns the cached collection schema when available, loading and caching it otherwise.
     * Insert/upsert callers may force a refresh when request construction indicates that the
     * cached schema is stale, and invalidate it when the server returns SchemaMismatch. Other
     * server errors preserve the cached schema.
     */
    private DescribeCollectionResponse getCollectionInfo(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                         String databaseName, String collectionName, boolean forceUpdate) {
        String dbName = actualDbName(databaseName);
        return SchemaCache.getInstance().getOrLoad(getEndpoint(), dbName, collectionName, forceUpdate, this,
                () -> describeCollection(blockingStub, dbName, collectionName));
    }

    private CompletableFuture<DescribeCollectionResponse> getCollectionInfoAsync(
            MilvusServiceGrpc.MilvusServiceFutureStub futureStub,
            String databaseName, String collectionName, boolean forceUpdate) {
        String dbName = actualDbName(databaseName);
        return SchemaCache.getInstance().getOrLoadAsync(
                getEndpoint(), dbName, collectionName, forceUpdate, this,
                () -> describeCollection(futureStub, dbName, collectionName));
    }

    private InsertRequest buildInsertRequest(InsertReq request, DescribeCollectionResponse descResp) {
        DataUtils.InsertBuilderWrapper requestBuilder = new DataUtils.InsertBuilderWrapper();
        DescribeCollectionResp descColl = convertUtils.convertDescCollectionResp(descResp);
        InsertRequest rpcRequest = requestBuilder.convertGrpcInsertRequest(request, descColl);
        return rpcRequest.toBuilder().setSchemaTimestamp(descResp.getUpdateTimestamp()).build();
    }

    public InsertResp insert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, InsertReq request) {
        return insert(blockingStub, request, true);
    }

    private InsertResp insert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, InsertReq request,
                              boolean allowRetry) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Insert to collection: '%s' in database: '%s'", collectionName, dbName);

        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, dbName, collectionName, false);

        // To handle this bug: https://github.com/milvus-io/milvus/issues/41688
        // if the collection is already recreated, some schema might be changed, the buildInsertRequest()
        // could not convert the InsertRequest with the old collectionDesc, we need to update the
        // collectionDesc and call buildInsertRequest() again.
        InsertRequest rpcRequest;
        try {
            rpcRequest = buildInsertRequest(request, descResp);
        } catch (DataNotMatchException ignored) {
            descResp = getCollectionInfo(blockingStub, dbName, collectionName, true);
            rpcRequest = buildInsertRequest(request, descResp);
        }

        // If there are multiple clients, the client_A repeatedly do insert, the client_B changes
        // the collection schema. The server might return a special error code "SchemaMismatch".
        // If the client_A gets this special error code, it needs to update the collectionDesc and
        // call insert() again.
        MutationResult response = blockingStub.insert(rpcRequest);
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SchemaMismatch) {
            invalidateSchemaCache(dbName, collectionName);
            if (allowRetry) {
                return insert(blockingStub, request, false);
            }
        }

        rpcUtils.handleResponse(title, response.getStatus());

        // update the last write timestamp for SESSION consistency
        updateTsCache(dbName, collectionName, response.getTimestamp());

        // handle integer pk or string pk
        List<Object> ids = new ArrayList<>();
        if (response.getIDs().hasIntId()) {
            ids = new ArrayList<>(response.getIDs().getIntId().getDataList());
        } else if (response.getIDs().hasStrId()) {
            ids = new ArrayList<>(response.getIDs().getStrId().getDataList());
        }
        return InsertResp.builder()
                .InsertCnt(response.getInsertCnt())
                .primaryKeys(ids)
                .cost(getCost(response.getStatus()))
                .build();
    }

    private UpsertRequest buildUpsertRequest(UpsertReq request, DescribeCollectionResponse descResp) {
        DataUtils.InsertBuilderWrapper requestBuilder = new DataUtils.InsertBuilderWrapper();
        DescribeCollectionResp descColl = convertUtils.convertDescCollectionResp(descResp);
        UpsertRequest rpcRequest = requestBuilder.convertGrpcUpsertRequest(request, descColl);
        return rpcRequest.toBuilder().setSchemaTimestamp(descResp.getUpdateTimestamp()).build();
    }

    public UpsertResp upsert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpsertReq request) {
        return upsert(blockingStub, request, true);
    }

    private UpsertResp upsert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpsertReq request,
                              boolean allowRetry) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Upsert to collection: '%s' in database: '%s'", collectionName, dbName);

        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, dbName, collectionName, false);

        // To handle this bug: https://github.com/milvus-io/milvus/issues/41688
        // if the collection is already recreated, some schema might be changed, the buildUpsertRequest()
        // could not convert the UpsertRequest with the old collectionDesc, we need to update the
        // collectionDesc and call buildUpsertRequest() again.
        UpsertRequest rpcRequest;
        try {
            rpcRequest = buildUpsertRequest(request, descResp);
        } catch (DataNotMatchException ignored) {
            descResp = getCollectionInfo(blockingStub, dbName, collectionName, true);
            rpcRequest = buildUpsertRequest(request, descResp);
        }

        // If there are multiple clients, the client_A repeatedly do upsert, the client_B changes
        // the collection schema. The server might return a special error code "SchemaMismatch".
        // If the client_A gets this special error code, it needs to update the collectionDesc and
        // call upsert() again.
        MutationResult response = blockingStub.upsert(rpcRequest);
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SchemaMismatch) {
            invalidateSchemaCache(dbName, collectionName);
            if (allowRetry) {
                return upsert(blockingStub, request, false);
            }
        }

        rpcUtils.handleResponse(title, response.getStatus());

        // update the last write timestamp for SESSION consistency
        updateTsCache(dbName, collectionName, response.getTimestamp());

        // handle integer pk or string pk
        List<Object> ids = new ArrayList<>();
        if (response.getIDs().hasIntId()) {
            ids = new ArrayList<>(response.getIDs().getIntId().getDataList());
        } else if (response.getIDs().hasStrId()) {
            ids = new ArrayList<>(response.getIDs().getStrId().getDataList());
        }
        return UpsertResp.builder()
                .upsertCnt(response.getUpsertCnt())
                .primaryKeys(ids)
                .cost(getCost(response.getStatus()))
                .build();
    }

    private QueryRequest buildQueryRequest(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                           QueryReq request) {
        validateQueryRequest(request);
        DescribeCollectionResponse descResp = null;
        if (CollectionUtils.isNotEmpty(request.getIds())) {
            descResp = getCollectionInfo(blockingStub, request.getDatabaseName(),
                    request.getCollectionName(), false);
        }
        QueryRequest queryRequest = buildBaseQueryRequest(request);
        if (descResp != null) {
            queryRequest = queryRequest.toBuilder()
                    .setExpr(vectorUtils.getExprById(getPrimaryKeyName(descResp), request.getIds()))
                    .build();
        }
        return queryRequest;
    }

    public QueryResp query(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, QueryReq request) {
        return query(blockingStub, request, null);
    }

    public QueryResp query(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                           QueryReq request, String clusterId) {
        QueryRequest queryRequest = withQueryClusterId(buildQueryRequest(blockingStub, request), clusterId);
        String title = String.format("Query collection: '%s' in database: '%s'",
                queryRequest.getCollectionName(), queryRequest.getDbName());
        QueryResults response = blockingStub.query(queryRequest);
        return convertQueryResponse(title, response);
    }

    public CompletableFuture<QueryResp> queryAsync(
            Supplier<MilvusServiceGrpc.MilvusServiceFutureStub> futureStubSupplier,
            QueryReq request, String clusterId, RpcUtils retryUtils) {
        final QueryRequest baseRequest;
        final List<Object> ids;
        try {
            validateQueryRequest(request);
            baseRequest = withQueryClusterId(buildBaseQueryRequest(request), clusterId);
            ids = CollectionUtils.isEmpty(request.getIds())
                    ? null : Collections.unmodifiableList(new ArrayList<>(request.getIds()));
        } catch (Throwable throwable) {
            return failedFuture(throwable);
        }

        return retryUtils.retryAsync(() -> queryAsync(
                futureStubSupplier.get(), baseRequest, ids));
    }

    private CompletableFuture<QueryResp> queryAsync(
            MilvusServiceGrpc.MilvusServiceFutureStub futureStub,
            QueryRequest baseRequest, List<Object> ids) {
        CompletableFuture<QueryRequest> requestFuture;
        if (ids == null) {
            requestFuture = CompletableFuture.completedFuture(baseRequest);
        } else {
            requestFuture = transformFuture(
                    getCollectionInfoAsync(futureStub, baseRequest.getDbName(),
                            baseRequest.getCollectionName(), false),
                    descResp -> baseRequest.toBuilder()
                            .setExpr(vectorUtils.getExprById(getPrimaryKeyName(descResp), ids))
                            .build());
        }

        return composeFuture(requestFuture, queryRequest -> {
            String title = String.format("Query collection: '%s' in database: '%s'",
                    queryRequest.getCollectionName(), queryRequest.getDbName());
            return transformFuture(futureStub.query(queryRequest),
                    response -> convertQueryResponse(title, response));
        });
    }

    private String getPrimaryKeyName(DescribeCollectionResponse descResp) {
        for (FieldSchema field : descResp.getSchema().getFieldsList()) {
            if (field.getIsPrimaryKey()) {
                return field.getName();
            }
        }
        throw new MilvusClientException(ErrorCode.SERVER_ERROR,
                "cannot find the primary key field in collection schema");
    }

    private void validateQueryRequest(QueryReq request) {
        if (StringUtils.isNotEmpty(request.getFilter()) && CollectionUtils.isNotEmpty(request.getIds())) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be set at the same time");
        }
    }

    private QueryRequest buildBaseQueryRequest(QueryReq request) {
        return vectorUtils.ConvertToGrpcQueryRequest(request).toBuilder()
                .setDbName(actualDbName(request.getDatabaseName()))
                .build();
    }

    private QueryResp convertQueryResponse(String title, QueryResults response) {
        rpcUtils.handleResponse(title, response.getStatus());

        return QueryResp.builder()
                .queryResults(convertUtils.getEntities(response))
                .sessionTs(response.getSessionTs())
                .cost(getCost(response.getStatus()))
                .build();
    }

    private long getCost(Status status) {
        String cost = status.getExtraInfoMap().get("report_value");
        if (StringUtils.isNotEmpty(cost)) {
            try {
                return Long.parseLong(cost);
            } catch (NumberFormatException e) {
                logger.warn("Failed to parse report_value as cost: {}", cost);
            }
        }
        return 0L;
    }

    public SearchResp search(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, SearchReq request) {
        return search(blockingStub, request, null);
    }

    public SearchResp search(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                             SearchReq request, String clusterId) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Search collection: '%s' in database: '%s'", collectionName, dbName);

        //checkCollectionExist(blockingStub, request.getCollectionName());

        // reset the db name so that the timestamp cache can set correct key for this collection
        request.setDatabaseName(actualDbName(dbName));
        SearchRequest searchRequest = vectorUtils.ConvertToGrpcSearchRequest(request);
        SearchRequest effectiveRequest = withSearchClusterId(searchRequest, clusterId);

        return convertSearchResponse(title, blockingStub.search(effectiveRequest), true);
    }

    public CompletableFuture<SearchResp> searchAsync(
            Supplier<MilvusServiceGrpc.MilvusServiceFutureStub> futureStubSupplier,
            SearchReq request, String clusterId, RpcUtils retryUtils) {
        final SearchRequest searchRequest;
        try {
            searchRequest = vectorUtils.ConvertToGrpcSearchRequest(request).toBuilder()
                    .setDbName(actualDbName(request.getDatabaseName()))
                    .build();
        } catch (Throwable throwable) {
            return failedFuture(throwable);
        }
        final SearchRequest effectiveRequest = withSearchClusterId(searchRequest, clusterId);
        String title = String.format("Search collection: '%s' in database: '%s'",
                effectiveRequest.getCollectionName(), effectiveRequest.getDbName());
        return retryUtils.retryAsync(() -> transformFuture(
                futureStubSupplier.get().search(effectiveRequest),
                response -> convertSearchResponse(title, response, true)));
    }

    private SearchResp convertSearchResponse(String title, SearchResults response, boolean includeAggregations) {
        rpcUtils.handleResponse(title, response.getStatus());

        SearchResp.SearchRespBuilder respBuilder = SearchResp.builder()
                .searchResults(convertUtils.getEntities(response))
                .sessionTs(response.getSessionTs())
                .recalls(response.getResults().getRecallsList())
                .cost(getCost(response.getStatus()));
        if (includeAggregations) {
            respBuilder.aggregationBuckets(convertUtils.getAggregationBuckets(response));
        }
        fillSearchRespFromExtraInfo(respBuilder, response.getStatus().getExtraInfoMap());
        return respBuilder.build();
    }

    public SearchResp hybridSearch(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, HybridSearchReq request) {
        return hybridSearch(blockingStub, request, null);
    }

    public SearchResp hybridSearch(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                   HybridSearchReq request, String clusterId) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Hybrid search collection: '%s' in database: '%s'", collectionName, dbName);

        //checkCollectionExist(blockingStub, request.getCollectionName());

        // reset the db name so that the timestamp cache can set correct key for this collection
        request.setDatabaseName(actualDbName(dbName));
        HybridSearchRequest searchRequest = vectorUtils.ConvertToGrpcHybridSearchRequest(request);
        HybridSearchRequest effectiveRequest = withHybridSearchClusterId(searchRequest, clusterId);

        return convertSearchResponse(title, blockingStub.hybridSearch(effectiveRequest), false);
    }

    public CompletableFuture<SearchResp> hybridSearchAsync(
            Supplier<MilvusServiceGrpc.MilvusServiceFutureStub> futureStubSupplier,
            HybridSearchReq request, String clusterId, RpcUtils retryUtils) {
        final HybridSearchRequest searchRequest;
        try {
            searchRequest = vectorUtils.ConvertToGrpcHybridSearchRequest(request).toBuilder()
                    .setDbName(actualDbName(request.getDatabaseName()))
                    .build();
        } catch (Throwable throwable) {
            return failedFuture(throwable);
        }
        final HybridSearchRequest effectiveRequest = withHybridSearchClusterId(searchRequest, clusterId);
        String title = String.format("Hybrid search collection: '%s' in database: '%s'",
                effectiveRequest.getCollectionName(), effectiveRequest.getDbName());
        return retryUtils.retryAsync(() -> transformFuture(
                futureStubSupplier.get().hybridSearch(effectiveRequest),
                response -> convertSearchResponse(title, response, false)));
    }

    private QueryRequest withQueryClusterId(QueryRequest request, String clusterId) {
        if (StringUtils.isEmpty(clusterId)) {
            return request;
        }
        QueryRequest.Builder builder = request.toBuilder().clearQueryParams();
        request.getQueryParamsList().stream()
                .filter(param -> !Constant.CLUSTER_ID.equals(param.getKey()))
                .forEach(builder::addQueryParams);
        return builder.addQueryParams(KeyValuePair.newBuilder()
                        .setKey(Constant.CLUSTER_ID)
                        .setValue(clusterId)
                        .build())
                .build();
    }

    private SearchRequest withSearchClusterId(SearchRequest request, String clusterId) {
        if (StringUtils.isEmpty(clusterId)) {
            return request;
        }
        SearchRequest.Builder builder = request.toBuilder().clearSearchParams();
        request.getSearchParamsList().stream()
                .filter(param -> !Constant.CLUSTER_ID.equals(param.getKey()))
                .forEach(builder::addSearchParams);
        return builder.addSearchParams(KeyValuePair.newBuilder()
                        .setKey(Constant.CLUSTER_ID)
                        .setValue(clusterId)
                        .build())
                .build();
    }

    private HybridSearchRequest withHybridSearchClusterId(
            HybridSearchRequest request, String clusterId) {
        if (StringUtils.isEmpty(clusterId)) {
            return request;
        }
        HybridSearchRequest.Builder builder = request.toBuilder().clearRankParams();
        request.getRankParamsList().stream()
                .filter(param -> !Constant.CLUSTER_ID.equals(param.getKey()))
                .forEach(builder::addRankParams);
        return builder.addRankParams(KeyValuePair.newBuilder()
                        .setKey(Constant.CLUSTER_ID)
                        .setValue(clusterId)
                        .build())
                .build();
    }

    private <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    private <T, R> CompletableFuture<R> transformFuture(ListenableFuture<T> source,
                                                        Function<T, R> converter) {
        CompletableFuture<R> target = new CompletableFuture<R>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean cancelled = super.cancel(mayInterruptIfRunning);
                if (cancelled) {
                    source.cancel(mayInterruptIfRunning);
                }
                return cancelled;
            }
        };
        Futures.addCallback(source, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                if (target.isDone()) {
                    return;
                }
                try {
                    target.complete(converter.apply(result));
                } catch (Throwable throwable) {
                    target.completeExceptionally(throwable);
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                target.completeExceptionally(throwable);
            }
        }, MoreExecutors.directExecutor());
        return target;
    }

    private <T, R> CompletableFuture<R> transformFuture(CompletableFuture<T> source,
                                                        Function<T, R> converter) {
        CompletableFuture<R> target = new CompletableFuture<R>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean cancelled = super.cancel(mayInterruptIfRunning);
                if (cancelled) {
                    source.cancel(mayInterruptIfRunning);
                }
                return cancelled;
            }
        };
        source.whenComplete((value, throwable) -> {
            if (target.isDone()) {
                return;
            }
            if (throwable != null) {
                target.completeExceptionally(throwable);
                return;
            }
            try {
                target.complete(converter.apply(value));
            } catch (Throwable conversionFailure) {
                target.completeExceptionally(conversionFailure);
            }
        });
        return target;
    }

    private <T, R> CompletableFuture<R> composeFuture(
            CompletableFuture<T> source, Function<T, CompletableFuture<R>> composer) {
        AtomicReference<CompletableFuture<?>> nextFuture = new AtomicReference<>();
        CompletableFuture<R> target = new CompletableFuture<R>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean cancelled = super.cancel(mayInterruptIfRunning);
                if (cancelled) {
                    source.cancel(mayInterruptIfRunning);
                    CompletableFuture<?> next = nextFuture.get();
                    if (next != null) {
                        next.cancel(mayInterruptIfRunning);
                    }
                }
                return cancelled;
            }
        };
        source.whenComplete((value, throwable) -> {
            if (target.isDone()) {
                return;
            }
            if (throwable != null) {
                target.completeExceptionally(throwable);
                return;
            }

            CompletableFuture<R> next;
            try {
                next = composer.apply(value);
                if (next == null) {
                    throw new NullPointerException("Future composer returned null future");
                }
            } catch (Throwable compositionFailure) {
                target.completeExceptionally(compositionFailure);
                return;
            }
            nextFuture.set(next);
            if (target.isCancelled()) {
                next.cancel(true);
                return;
            }
            next.whenComplete((result, nextFailure) -> {
                if (nextFailure == null) {
                    target.complete(result);
                } else {
                    target.completeExceptionally(nextFailure);
                }
            });
        });
        return target;
    }

    private void fillSearchRespFromExtraInfo(SearchResp.SearchRespBuilder respBuilder, java.util.Map<String, String> extraInfo) {
        if (extraInfo.containsKey("scanned_remote_bytes")) {
            respBuilder.scannedRemoteBytes(Long.parseLong(extraInfo.get("scanned_remote_bytes")));
        }
        if (extraInfo.containsKey("scanned_total_bytes")) {
            respBuilder.scannedTotalBytes(Long.parseLong(extraInfo.get("scanned_total_bytes")));
        }
        if (extraInfo.containsKey("cache_hit_ratio")) {
            respBuilder.cacheHitRatio(Float.parseFloat(extraInfo.get("cache_hit_ratio")));
        }
    }

    public QueryIterator queryIterator(RpcStubWrapper blockingStub,
                                       QueryIteratorReq request) {
        return queryIterator(blockingStub, request, null);
    }

    public QueryIterator queryIterator(RpcStubWrapper blockingStub,
                                       QueryIteratorReq request, String clusterId) {
        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub.get(), request.getDatabaseName(),
                request.getCollectionName(), false);
        DescribeCollectionResp respR = convertUtils.convertDescCollectionResp(descResp);
        CreateCollectionReq.FieldSchema pkField = respR.getCollectionSchema().getField(respR.getPrimaryFieldName());
        return new QueryIterator(request, blockingStub, pkField, respR.getCollectionID(), clusterId);
    }

    public SearchIterator searchIterator(RpcStubWrapper blockingStub,
                                         SearchIteratorReq request) {
        return searchIterator(blockingStub, request, null);
    }

    public SearchIterator searchIterator(RpcStubWrapper blockingStub,
                                         SearchIteratorReq request, String clusterId) {
        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub.get(), request.getDatabaseName(),
                request.getCollectionName(), false);
        DescribeCollectionResp respR = convertUtils.convertDescCollectionResp(descResp);
        CreateCollectionReq.FieldSchema pkField = respR.getCollectionSchema().getField(respR.getPrimaryFieldName());
        return new SearchIterator(request, blockingStub, pkField, clusterId);
    }

    public SearchIteratorV2 searchIteratorV2(RpcStubWrapper blockingStub,
                                             SearchIteratorReqV2 request) {
        return searchIteratorV2(blockingStub, request, null);
    }

    public SearchIteratorV2 searchIteratorV2(RpcStubWrapper blockingStub,
                                             SearchIteratorReqV2 request, String clusterId) {
        return new SearchIteratorV2(request, blockingStub, clusterId);
    }

    public DeleteResp delete(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DeleteReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Delete entities of collection: '%s' in database: '%s'", collectionName, dbName);

        if (request.getFilter() != null && request.getIds() != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be set at the same time");
        }

        if (request.getFilter() == null) {
            DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, dbName, collectionName, false);
            DescribeCollectionResp respR = convertUtils.convertDescCollectionResp(descResp);
            request.setFilter(vectorUtils.getExprById(respR.getPrimaryFieldName(), request.getIds()));
        }
        DeleteRequest rpcRequest = dataUtils.ConvertToGrpcDeleteRequest(request);
        MutationResult response = blockingStub.delete(rpcRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        // update the last write timestamp for SESSION consistency
        updateTsCache(dbName, collectionName, response.getTimestamp());

        return DeleteResp.builder()
                .deleteCnt(response.getDeleteCnt())
                .cost(getCost(response.getStatus()))
                .build();
    }

    public GetResp get(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetReq request) {
        return get(blockingStub, request, null);
    }

    public GetResp get(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                       GetReq request, String clusterId) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Get entities of collection: '%s' in database: '%s'", collectionName, dbName);
        logger.debug(title);
        // call query to get the result
        QueryResp queryResp = query(blockingStub, toQueryReq(request), clusterId);

        return GetResp.builder()
                .getResults(queryResp.getQueryResults())
                .build();
    }

    public CompletableFuture<GetResp> getAsync(
            Supplier<MilvusServiceGrpc.MilvusServiceFutureStub> futureStubSupplier,
            GetReq request, String clusterId, RpcUtils retryUtils) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("Get entities of collection: '%s' in database: '%s'", collectionName, dbName);
        logger.debug(title);
        final QueryReq queryReq;
        try {
            queryReq = toQueryReq(request);
        } catch (Throwable throwable) {
            return failedFuture(throwable);
        }
        // call queryAsync to get the result
        return transformFuture(
                queryAsync(futureStubSupplier, queryReq, clusterId, retryUtils),
                queryResp -> GetResp.builder()
                        .getResults(queryResp.getQueryResults())
                        .build());
    }

    private QueryReq toQueryReq(GetReq request) {
        QueryReq.QueryReqBuilder queryReqBuilder = QueryReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .ids(request.getIds());
        List<String> partitionNames = new ArrayList<>();
        if (StringUtils.isNotEmpty(request.getPartitionName())) {
            partitionNames.add(request.getPartitionName());
        }
        if (request.getPartitionNames() != null) {
            partitionNames.addAll(request.getPartitionNames());
        }
        if (!partitionNames.isEmpty()) {
            // deduplicate while preserving order (a caller may set both partitionName and partitionNames)
            queryReqBuilder.partitionNames(new ArrayList<>(new LinkedHashSet<>(partitionNames)));
        }
        QueryReq queryReq = queryReqBuilder.build();
        if (request.getOutputFields() != null) {
            queryReq.setOutputFields(request.getOutputFields());
        }
        return queryReq;
    }

    public RunAnalyzerResp runAnalyzer(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, RunAnalyzerReq request) {
        String title = "RunAnalyzer";
        if (request.getTexts().isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Texts list is empty.");
        }

        RunAnalyzerRequest.Builder builder = RunAnalyzerRequest.newBuilder();
        List<ByteString> byteStrings = new ArrayList<>();
        for (String text : request.getTexts()) {
            byteStrings.add(ByteString.copyFrom(text.getBytes()));
        }

        List<String> analyzerNames = request.getAnalyzerNames();
        builder.addAllAnalyzerNames(analyzerNames);

        String params = JsonUtils.toJson(request.getAnalyzerParams());
        logger.debug(params);
        RunAnalyzerRequest.Builder runRequestBuilder = builder.addAllPlaceholder(byteStrings)
                .setWithDetail(request.getWithDetail())
                .setWithHash(request.getWithHash())
                .setDbName(actualDbName(request.getDatabaseName()))
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName());
        if (request.getAnalyzerParams() != null && !request.getAnalyzerParams().isEmpty()) {
            runRequestBuilder.setAnalyzerParams(params);
        }
        RunAnalyzerRequest runRequest = runRequestBuilder.build();
        RunAnalyzerResponse response = blockingStub.runAnalyzer(runRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        List<RunAnalyzerResp.AnalyzerResult> toResults = new ArrayList<>();
        List<AnalyzerResult> results = response.getResultsList();
        results.forEach((item) -> {
            List<RunAnalyzerResp.AnalyzerToken> toTokens = new ArrayList<>();
            List<AnalyzerToken> tokens = item.getTokensList();
            tokens.forEach((token) -> {
                toTokens.add(RunAnalyzerResp.AnalyzerToken.builder()
                        .token(token.getToken())
                        .startOffset(token.getStartOffset())
                        .endOffset(token.getEndOffset())
                        .position(token.getPosition())
                        .positionLength(token.getPositionLength())
                        .hash(token.getHash() & 0xFFFFFFFFL)
                        .build());
            });
            toResults.add(RunAnalyzerResp.AnalyzerResult.builder()
                    .tokens(toTokens)
                    .build());
        });

        return RunAnalyzerResp.builder()
                .results(toResults)
                .build();
    }
}

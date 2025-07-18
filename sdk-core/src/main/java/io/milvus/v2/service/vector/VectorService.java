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

import com.google.protobuf.ByteString;
import io.milvus.common.utils.GTsDict;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.orm.iterator.*;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.BaseService;
import io.milvus.v2.service.collection.CollectionService;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.response.*;
import io.milvus.v2.utils.DataUtils;
import io.milvus.v2.utils.VectorUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VectorService extends BaseService {
    Logger logger = LoggerFactory.getLogger(VectorService.class);
    public CollectionService collectionService = new CollectionService();
    private ConcurrentHashMap<String, DescribeCollectionResponse> cacheCollectionInfo = new ConcurrentHashMap<>();

    private DescribeCollectionResponse describeCollection(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                          String databaseName, String collectionName) {
        String msg = String.format("Fail to describe collection '%s'", collectionName);
        DescribeCollectionRequest.Builder builder = DescribeCollectionRequest.newBuilder()
                .setCollectionName(collectionName);
        if (StringUtils.isNotEmpty(databaseName)) {
            builder.setDbName(databaseName);
            msg = String.format("Fail to describe collection '%s' in database '%s'",
                    collectionName, databaseName);
        }
        DescribeCollectionRequest describeCollectionRequest = builder.build();
        DescribeCollectionResponse response = blockingStub.describeCollection(describeCollectionRequest);
        rpcUtils.handleResponse(msg, response.getStatus());
        return response;
    }

    /**
     * This method is for insert/upsert requests to reduce the rpc call of describeCollection()
     * Always try to get the collection info from cache.
     * If the cache doesn't have the collection info, call describeCollection() and cache it.
     * If insert/upsert get server error, remove the cached collection info.
     */
    private DescribeCollectionResponse getCollectionInfo(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                                         String databaseName, String collectionName, boolean forceUpdate) {
        String key = GTsDict.CombineCollectionName(actualDbName(databaseName), collectionName);
        DescribeCollectionResponse info = cacheCollectionInfo.get(key);
        if (info == null || forceUpdate) {
            info = describeCollection(blockingStub, databaseName, collectionName);
            cacheCollectionInfo.put(key, info);
        }

        return info;
    }

    public void cleanCollectionCache() {
        cacheCollectionInfo.clear();
    }

    /**
     * insert/upsert return an error, but is not a RateLimit error,
     * clean the cache so that the next insert will call describeCollection() to get the latest info.
     */
    private void cleanCacheIfFailed(Status status, String databaseName, String collectionName) {
        if ((status.getCode() != 0 && status.getCode() != 8) ||
                (!status.getErrorCode().equals(io.milvus.grpc.ErrorCode.Success) &&
                        status.getErrorCode() != io.milvus.grpc.ErrorCode.RateLimit)) {
            removeCollectionCache(databaseName, collectionName);
        }
    }

    private void removeCollectionCache(String databaseName, String collectionName) {
        String key = GTsDict.CombineCollectionName(actualDbName(databaseName), collectionName);
        cacheCollectionInfo.remove(key);
    }

    private InsertRequest buildInsertRequest(InsertReq request, DescribeCollectionResponse descResp) {
        DataUtils.InsertBuilderWrapper requestBuilder = new DataUtils.InsertBuilderWrapper();
        DescribeCollectionResp descColl = convertUtils.convertDescCollectionResp(descResp);
        InsertRequest rpcRequest = requestBuilder.convertGrpcInsertRequest(request, descColl);
        return rpcRequest.toBuilder().setSchemaTimestamp(descResp.getUpdateTimestamp()).build();
    }

    public InsertResp insert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, InsertReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("InsertRequest collectionName:%s", collectionName);

        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, dbName, collectionName, false);

        // To handle this bug: https://github.com/milvus-io/milvus/issues/41688
        // if the collection is already recreated, some schema might be changed, the buildInsertRequest()
        // could not convert the InsertRequest with the old collectionDesc, we need to update the
        // collectionDesc and call buildInsertRequest() again.
        InsertRequest rpcRequest;
        try {
            rpcRequest = buildInsertRequest(request, descResp);
        } catch (Exception ignored) {
            descResp = getCollectionInfo(blockingStub, dbName, collectionName, true);
            rpcRequest = buildInsertRequest(request, descResp);
        }

        // If there are multiple clients, the client_A repeatedly do insert, the client_B changes
        // the collection schema. The server might return a special error code "SchemaMismatch".
        // If the client_A gets this special error code, it needs to update the collectionDesc and
        // call insert() again.
        MutationResult response = blockingStub.insert(rpcRequest);
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SchemaMismatch) {
            getCollectionInfo(blockingStub, dbName, collectionName, true);
            return this.insert(blockingStub, request);
        }

        // if illegal data, server fails to process insert, else succeed
        cleanCacheIfFailed(response.getStatus(), dbName, collectionName);
        rpcUtils.handleResponse(title, response.getStatus());

        // update the last write timestamp for SESSION consistency
        String key = GTsDict.CombineCollectionName(actualDbName(dbName), collectionName);
        GTsDict.getInstance().updateCollectionTs(key, response.getTimestamp());

        if (response.getIDs().hasIntId()) {
            List<Object> ids = new ArrayList<>(response.getIDs().getIntId().getDataList());
            return InsertResp.builder()
                    .InsertCnt(response.getInsertCnt())
                    .primaryKeys(ids)
                    .build();
        } else {
            List<Object> ids = new ArrayList<>(response.getIDs().getStrId().getDataList());
            return InsertResp.builder()
                    .InsertCnt(response.getInsertCnt())
                    .primaryKeys(ids)
                    .build();
        }
    }

    private UpsertRequest buildUpsertRequest(UpsertReq request, DescribeCollectionResponse descResp) {
        DataUtils.InsertBuilderWrapper requestBuilder = new DataUtils.InsertBuilderWrapper();
        DescribeCollectionResp descColl = convertUtils.convertDescCollectionResp(descResp);
        UpsertRequest rpcRequest = requestBuilder.convertGrpcUpsertRequest(request, descColl);
        return rpcRequest.toBuilder().setSchemaTimestamp(descResp.getUpdateTimestamp()).build();
    }

    public UpsertResp upsert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, UpsertReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("UpsertRequest collectionName:%s", collectionName);

        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, dbName, collectionName, false);

        // To handle this bug: https://github.com/milvus-io/milvus/issues/41688
        // if the collection is already recreated, some schema might be changed, the buildUpsertRequest()
        // could not convert the UpsertRequest with the old collectionDesc, we need to update the
        // collectionDesc and call buildUpsertRequest() again.
        UpsertRequest rpcRequest;
        try {
            rpcRequest = buildUpsertRequest(request, descResp);
        } catch (Exception ignored) {
            descResp = getCollectionInfo(blockingStub, dbName, collectionName, true);
            rpcRequest = buildUpsertRequest(request, descResp);
        }

        // If there are multiple clients, the client_A repeatedly do upsert, the client_B changes
        // the collection schema. The server might return a special error code "SchemaMismatch".
        // If the client_A gets this special error code, it needs to update the collectionDesc and
        // call upsert() again.
        MutationResult response = blockingStub.upsert(rpcRequest);
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SchemaMismatch) {
            getCollectionInfo(blockingStub, dbName, collectionName, true);
            return this.upsert(blockingStub, request);
        }

        // if illegal data, server fails to process upsert, clean the schema cache
        // so that the next call of dml can update the cache
        cleanCacheIfFailed(response.getStatus(), dbName, collectionName);
        rpcUtils.handleResponse(title, response.getStatus());

        // update the last write timestamp for SESSION consistency
        String key = GTsDict.CombineCollectionName(actualDbName(dbName), collectionName);
        GTsDict.getInstance().updateCollectionTs(key, response.getTimestamp());
        return UpsertResp.builder()
                .upsertCnt(response.getUpsertCnt())
                .build();
    }

    public QueryResp query(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, QueryReq request) {
        String title = String.format("QueryRequest collectionName:%s, databaseName:%s", request.getCollectionName(), request.getDatabaseName());
        if (request.getFilter() == null && request.getIds() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be null at the same time");
        } else if (request.getFilter() != null && request.getIds() != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be set at the same time");
        }


        if (request.getIds() != null && request.getFilter() == null) {
            DescribeCollectionReq descReq = DescribeCollectionReq.builder().databaseName(request.getDatabaseName()).collectionName(request.getCollectionName()).build();
            DescribeCollectionResp descResp = collectionService.describeCollection(blockingStub, descReq);
            request.setFilter(vectorUtils.getExprById(descResp.getPrimaryFieldName(), request.getIds()));
        }

        // reset the db name so that the timestamp cache can set correct key for this collection
        request.setDatabaseName(actualDbName(request.getDatabaseName()));
        QueryResults response = blockingStub.query(vectorUtils.ConvertToGrpcQueryRequest(request));
        rpcUtils.handleResponse(title, response.getStatus());

        return QueryResp.builder()
                .queryResults(convertUtils.getEntities(response))
                .sessionTs(response.getSessionTs())
                .build();

    }

    public SearchResp search(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, SearchReq request) {
        String title = String.format("SearchRequest collectionName:%s, databaseName:%s", request.getCollectionName(), request.getDatabaseName());

        //checkCollectionExist(blockingStub, request.getCollectionName());

        // reset the db name so that the timestamp cache can set correct key for this collection
        request.setDatabaseName(actualDbName(request.getDatabaseName()));
        SearchRequest searchRequest = vectorUtils.ConvertToGrpcSearchRequest(request);

        SearchResults response = blockingStub.search(searchRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return SearchResp.builder()
                .searchResults(convertUtils.getEntities(response))
                .sessionTs(response.getSessionTs())
                .recalls(response.getResults().getRecallsList())
                .build();
    }

    public SearchResp hybridSearch(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, HybridSearchReq request) {
        String title = String.format("HybridSearchRequest collectionName:%s", request.getCollectionName());

        //checkCollectionExist(blockingStub, request.getCollectionName());

        // reset the db name so that the timestamp cache can set correct key for this collection
        request.setDatabaseName(actualDbName(request.getDatabaseName()));
        HybridSearchRequest searchRequest = vectorUtils.ConvertToGrpcHybridSearchRequest(request);

        SearchResults response = blockingStub.hybridSearch(searchRequest);

        rpcUtils.handleResponse(title, response.getStatus());

        return SearchResp.builder()
                .searchResults(convertUtils.getEntities(response))
                .recalls(response.getResults().getRecallsList())
                .build();
    }

    public QueryIterator queryIterator(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                           QueryIteratorReq request) {
        DescribeCollectionResponse descResp = describeCollection(blockingStub, request.getDatabaseName(), request.getCollectionName());
        DescribeCollectionResp respR = convertUtils.convertDescCollectionResp(descResp);
        CreateCollectionReq.FieldSchema pkField = respR.getCollectionSchema().getField(respR.getPrimaryFieldName());
        return new QueryIterator(request, blockingStub, pkField);
    }

    public SearchIterator searchIterator(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                            SearchIteratorReq request) {
        DescribeCollectionResponse descResp = describeCollection(blockingStub, request.getDatabaseName(), request.getCollectionName());
        DescribeCollectionResp respR = convertUtils.convertDescCollectionResp(descResp);
        CreateCollectionReq.FieldSchema pkField = respR.getCollectionSchema().getField(respR.getPrimaryFieldName());
        return new SearchIterator(request, blockingStub, pkField);
    }

    public SearchIteratorV2 searchIteratorV2(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                                             SearchIteratorReqV2 request) {
        return new SearchIteratorV2(request, blockingStub);
    }

    public DeleteResp delete(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DeleteReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        String title = String.format("DeleteRequest collectionName:%s", collectionName);

        if (request.getFilter() != null && request.getIds() != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be set at the same time");
        }

        if (request.getFilter() == null) {
            DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, dbName, collectionName, false);
            DescribeCollectionResp respR = convertUtils.convertDescCollectionResp(descResp);
            request.setFilter(vectorUtils.getExprById(respR.getPrimaryFieldName(), request.getIds()));
        }
        DeleteRequest.Builder builder = DeleteRequest.newBuilder()
                .setCollectionName(collectionName)
                .setPartitionName(request.getPartitionName())
                .setExpr(request.getFilter());
        if (request.getFilter() != null && !request.getFilter().isEmpty()) {
            Map<String, Object> filterTemplateValues = request.getFilterTemplateValues();
            filterTemplateValues.forEach((key, value)->{
                builder.putExprTemplateValues(key, VectorUtils.deduceAndCreateTemplateValue(value));
            });
        }
        MutationResult response = blockingStub.delete(builder.build());

        // if illegal data, server fails to process delete, clean the schema cache
        // so that the next call of dml can update the cache
        cleanCacheIfFailed(response.getStatus(), dbName, collectionName);
        rpcUtils.handleResponse(title, response.getStatus());

        // update the last write timestamp for SESSION consistency
        String key = GTsDict.CombineCollectionName(actualDbName(dbName), collectionName);
        GTsDict.getInstance().updateCollectionTs(key, response.getTimestamp());
        return DeleteResp.builder()
                .deleteCnt(response.getDeleteCnt())
                .build();
    }

    public GetResp get(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, GetReq request) {
        String title = String.format("GetRequest collectionName:%s", request.getCollectionName());
        logger.debug(title);
        QueryReq queryReq = QueryReq.builder()
                .collectionName(request.getCollectionName())
                .ids(request.getIds())
                .build();
        if (request.getOutputFields() != null) {
            queryReq.setOutputFields(request.getOutputFields());
        }
        // call query to get the result
        QueryResp queryResp = query(blockingStub, queryReq);

        return GetResp.builder()
                .getResults(queryResp.getQueryResults())
                .build();
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
        System.out.println(params);
        RunAnalyzerRequest runRequest = builder.addAllPlaceholder(byteStrings)
                .setAnalyzerParams(params)
                .setWithDetail(request.getWithDetail())
                .setWithHash(request.getWithHash())
                .setDbName(request.getDatabaseName())
                .setCollectionName(request.getCollectionName())
                .setFieldName(request.getFieldName())
                .build();
        RunAnalyzerResponse response = blockingStub.runAnalyzer(runRequest);
        rpcUtils.handleResponse(title, response.getStatus());

        List<RunAnalyzerResp.AnalyzerResult> toResults = new ArrayList<>();
        List<AnalyzerResult> results = response.getResultsList();
        results.forEach((item)->{
            List<RunAnalyzerResp.AnalyzerToken> toTokens = new ArrayList<>();
            List<AnalyzerToken> tokens = item.getTokensList();
            tokens.forEach((token)->{
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

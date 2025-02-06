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

import io.milvus.common.utils.GTsDict;
import io.milvus.exception.ParamException;
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
        String key = combineCacheKey(databaseName, collectionName);
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

    private String combineCacheKey(String databaseName, String collectionName) {
        if (collectionName == null || StringUtils.isBlank(collectionName)) {
            throw new ParamException("Collection name is empty, not able to get collection info.");
        }
        String key = collectionName;
        if (StringUtils.isNotEmpty(databaseName)) {
            key = String.format("%s|%s", databaseName, collectionName);
        }
        return key;
    }

    /**
     * insert/upsert return an error, but is not a RateLimit error,
     * clean the cache so that the next insert will call describeCollection() to get the latest info.
     */
    private void cleanCacheIfFailed(Status status, String databaseName, String collectionName) {
        if ((status.getCode() != 0 && status.getCode() != 8) ||
                (!status.getErrorCode().equals(io.milvus.grpc.ErrorCode.Success) &&
                        status.getErrorCode() != io.milvus.grpc.ErrorCode.RateLimit)) {
            cacheCollectionInfo.remove(combineCacheKey(databaseName, collectionName));
        }
    }

    private InsertRequest buildInsertRequest(InsertReq request, DescribeCollectionResponse descResp) {
        DataUtils.InsertBuilderWrapper requestBuilder = new DataUtils.InsertBuilderWrapper();
        DescribeCollectionResp descColl = convertUtils.convertDescCollectionResp(descResp);
        InsertRequest rpcRequest = requestBuilder.convertGrpcInsertRequest(request, descColl);
        return rpcRequest.toBuilder().setSchemaTimestamp(descResp.getUpdateTimestamp()).build();
    }

    public InsertResp insert(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, InsertReq request) {
        String title = String.format("InsertRequest collectionName:%s", request.getCollectionName());

        // TODO: set the database name
        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, "", request.getCollectionName(), false);
        InsertRequest rpcRequest = buildInsertRequest(request, descResp);
        MutationResult response = blockingStub.insert(rpcRequest);
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SchemaMismatch) {
            descResp = getCollectionInfo(blockingStub, "", request.getCollectionName(), true);
            rpcRequest =  buildInsertRequest(request, descResp);
            response = blockingStub.insert(rpcRequest);
        }

        cleanCacheIfFailed(response.getStatus(), "", request.getCollectionName());
        rpcUtils.handleResponse(title, response.getStatus());
        GTsDict.getInstance().updateCollectionTs(request.getCollectionName(), response.getTimestamp());

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
        String title = String.format("UpsertRequest collectionName:%s", request.getCollectionName());

        // TODO: set the database name
        DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, "", request.getCollectionName(), false);
        UpsertRequest rpcRequest = buildUpsertRequest(request, descResp);
        MutationResult response = blockingStub.upsert(rpcRequest);
        if (response.getStatus().getErrorCode() == io.milvus.grpc.ErrorCode.SchemaMismatch) {
            descResp = getCollectionInfo(blockingStub, "", request.getCollectionName(), true);
            rpcRequest =  buildUpsertRequest(request, descResp);
            response = blockingStub.upsert(rpcRequest);
        }

        cleanCacheIfFailed(response.getStatus(), "", request.getCollectionName());
        rpcUtils.handleResponse(title, response.getStatus());
        GTsDict.getInstance().updateCollectionTs(request.getCollectionName(), response.getTimestamp());
        return UpsertResp.builder()
                .upsertCnt(response.getInsertCnt())
                .build();
    }

    public QueryResp query(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, QueryReq request) {
        String title = String.format("QueryRequest collectionName:%s", request.getCollectionName());
        if (request.getFilter() == null && request.getIds() == null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be null at the same time");
        } else if (request.getFilter() != null && request.getIds() != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be set at the same time");
        }


        if (request.getIds() != null && request.getFilter() == null) {
            DescribeCollectionReq descReq = DescribeCollectionReq.builder().collectionName(request.getCollectionName()).build();
            DescribeCollectionResp descResp = collectionService.describeCollection(blockingStub, descReq);
            request.setFilter(vectorUtils.getExprById(descResp.getPrimaryFieldName(), request.getIds()));
        }
        QueryResults response = blockingStub.query(vectorUtils.ConvertToGrpcQueryRequest(request));
        rpcUtils.handleResponse(title, response.getStatus());

        return QueryResp.builder()
                .queryResults(convertUtils.getEntities(response))
                .sessionTs(response.getSessionTs())
                .build();

    }

    public SearchResp search(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, SearchReq request) {
        String title = String.format("SearchRequest collectionName:%s", request.getCollectionName());

        //checkCollectionExist(blockingStub, request.getCollectionName());

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

    public DeleteResp delete(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, DeleteReq request) {
        String title = String.format("DeleteRequest collectionName:%s", request.getCollectionName());

        if (request.getFilter() != null && request.getIds() != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "filter and ids can't be set at the same time");
        }

        if (request.getFilter() == null) {
            DescribeCollectionResponse descResp = getCollectionInfo(blockingStub, "", request.getCollectionName(), false);
            DescribeCollectionResp respR = convertUtils.convertDescCollectionResp(descResp);
            request.setFilter(vectorUtils.getExprById(respR.getPrimaryFieldName(), request.getIds()));
        }
        DeleteRequest.Builder builder = DeleteRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .setPartitionName(request.getPartitionName())
                .setExpr(request.getFilter());
        if (request.getFilter() != null && !request.getFilter().isEmpty()) {
            Map<String, Object> filterTemplateValues = request.getFilterTemplateValues();
            filterTemplateValues.forEach((key, value)->{
                builder.putExprTemplateValues(key, VectorUtils.deduceAndCreateTemplateValue(value));
            });
        }
        MutationResult response = blockingStub.delete(builder.build());
        rpcUtils.handleResponse(title, response.getStatus());
        GTsDict.getInstance().updateCollectionTs(request.getCollectionName(), response.getTimestamp());
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
}

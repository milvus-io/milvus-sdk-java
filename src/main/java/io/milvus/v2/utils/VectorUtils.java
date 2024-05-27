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

package io.milvus.v2.utils;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.MetricType;
import io.milvus.param.ParamUtils;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.ranker.BaseRanker;
import io.milvus.v2.service.vector.request.data.*;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class VectorUtils {
    private static final Gson GSON_INSTANCE = new Gson();

    public QueryRequest ConvertToGrpcQueryRequest(QueryReq request){
        QueryRequest.Builder builder = QueryRequest.newBuilder()
                .setCollectionName(request.getCollectionName())
                .addAllPartitionNames(request.getPartitionNames())
                .addAllOutputFields(request.getOutputFields())
                .setExpr(request.getFilter());

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (request.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }

        // set offset and limit value.
        // directly pass the two values, the server will verify them.
        long offset = request.getOffset();
        if (offset > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.OFFSET)
                    .setValue(String.valueOf(offset))
                    .build());
        }

        long limit = request.getLimit();
        if (limit > 0) {
            builder.addQueryParams(KeyValuePair.newBuilder()
                    .setKey(Constant.LIMIT)
                    .setValue(String.valueOf(limit))
                    .build());
        }

        // ignore growing
//        builder.addQueryParams(KeyValuePair.newBuilder()
//                .setKey(Constant.IGNORE_GROWING)
//                .setValue(String.valueOf(request.isIgnoreGrowing()))
//                .build());

        return builder.build();

    }

    private static long getGuaranteeTimestamp(ConsistencyLevel consistencyLevel,
                                              long guaranteeTimestamp, Long gracefulTime){
        if(consistencyLevel == null){
            return 1L;
        }
        switch (consistencyLevel){
            case STRONG:
                guaranteeTimestamp = 0L;
                break;
            case BOUNDED:
                guaranteeTimestamp = (new Date()).getTime() - gracefulTime;
                break;
            case EVENTUALLY:
                guaranteeTimestamp = 1L;
                break;
        }
        return guaranteeTimestamp;
    }

    public SearchRequest ConvertToGrpcSearchRequest(SearchReq request) {
        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setCollectionName(request.getCollectionName());
        if (!request.getPartitionNames().isEmpty()) {
            request.getPartitionNames().forEach(builder::addPartitionNames);
        }


        // prepare target vectors
        List<BaseVector> vectors = request.getData();
        if (vectors.isEmpty()) {
            throw new ParamException("Target vectors list of search request is empty.");
        }
        PlaceholderType plType = vectors.get(0).getPlaceholderType();
        List<Object> data = new ArrayList<>();
        for (BaseVector vector : vectors) {
            if (vector.getPlaceholderType() != plType) {
                throw new ParamException("Different types of target vectors in a search request is not allowed.");
            }
            data.add(vector.getData());
        }

        ByteString byteStr = ParamUtils.convertPlaceholder(data, plType);
        builder.setPlaceholderGroup(byteStr);

        // search parameters
        builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.VECTOR_FIELD)
                                .setValue(request.getAnnsField())
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(request.getTopK()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.ROUND_DECIMAL)
                                .setValue(String.valueOf(request.getRoundDecimal()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.IGNORE_GROWING)
                                .setValue(String.valueOf(request.isIgnoreGrowing()))
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                        .setKey(Constant.OFFSET)
                        .setValue(String.valueOf(request.getOffset()))
                        .build());


        if (null != request.getSearchParams()) {
            try {
                String searchParams = GSON_INSTANCE.toJson(request.getSearchParams());
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.PARAMS)
                                .setValue(searchParams)
                                .build());
            } catch (IllegalArgumentException e) {
                throw new ParamException(e.getMessage() + e.getCause().getMessage());
            }
        }

        if (!request.getOutputFields().isEmpty()) {
            request.getOutputFields().forEach(builder::addOutputFields);
        }

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (request.getFilter() != null && !request.getFilter().isEmpty()) {
            builder.setDsl(request.getFilter());
        }

        long guaranteeTimestamp = getGuaranteeTimestamp(request.getConsistencyLevel(),
                request.getGuaranteeTimestamp(), request.getGracefulTime());
        //builder.setTravelTimestamp(request.getTravelTimestamp());
        builder.setGuaranteeTimestamp(guaranteeTimestamp);

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (request.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }

        return builder.build();
    }

    public static SearchRequest convertAnnSearchParam(@NonNull AnnSearchReq annSearchReq,
                                                      ConsistencyLevel consistencyLevel) {
        SearchRequest.Builder builder = SearchRequest.newBuilder();
        // prepare target vectors
        List<BaseVector> vectors = annSearchReq.getVectors();
        if (vectors.isEmpty()) {
            throw new ParamException("Target vectors list of search request is empty.");
        }
        PlaceholderType plType = vectors.get(0).getPlaceholderType();
        List<Object> data = new ArrayList<>();
        for (BaseVector vector : vectors) {
            if (vector.getPlaceholderType() != plType) {
                throw new ParamException("Different types of target vectors in a search request is not allowed.");
            }
            data.add(vector.getData());
        }

        ByteString byteStr = ParamUtils.convertPlaceholder(data, plType);
        builder.setPlaceholderGroup(byteStr);
        builder.setNq(vectors.size());

        builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.VECTOR_FIELD)
                                .setValue(annSearchReq.getVectorFieldName())
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(annSearchReq.getTopK()))
                                .build());

        // params
        String params = "{}";
        if (null != annSearchReq.getParams() && !annSearchReq.getParams().isEmpty()) {
            params = annSearchReq.getParams();
        }
        builder.addSearchParams(KeyValuePair.newBuilder()
                        .setKey(Constant.PARAMS)
                        .setValue(params)
                        .build());

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (annSearchReq.getExpr() != null && !annSearchReq.getExpr().isEmpty()) {
            builder.setDsl(annSearchReq.getExpr());
        }

        if (consistencyLevel == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(consistencyLevel.getCode());
        }

        return builder.build();
    }

    public HybridSearchRequest ConvertToGrpcHybridSearchRequest(HybridSearchReq request) {
        HybridSearchRequest.Builder builder = HybridSearchRequest.newBuilder()
                .setCollectionName(request.getCollectionName());

        if (request.getPartitionNames() != null && !request.getPartitionNames().isEmpty()) {
            request.getPartitionNames().forEach(builder::addPartitionNames);
        }
        if (StringUtils.isNotEmpty(request.getDatabaseName())) {
            builder.setDbName(request.getDatabaseName());
        }

        if (request.getSearchRequests() == null || request.getSearchRequests().isEmpty()) {
            throw new ParamException("Sub-request list is empty.");
        }

        for (AnnSearchReq req : request.getSearchRequests()) {
            SearchRequest searchRequest = convertAnnSearchParam(req, request.getConsistencyLevel());
            builder.addRequests(searchRequest);
        }

        // set ranker
        BaseRanker ranker = request.getRanker();
        if (request.getRanker() == null) {
            throw new ParamException("Ranker is null.");
        }

        Map<String, String> props = ranker.getProperties();
        props.put("limit", String.format("%d", request.getTopK()));
        props.put("round_decimal", String.format("%d", request.getRoundDecimal()));
        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(props);
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addRankParams);
        }

        // output fields
        if (request.getOutFields() != null && !request.getOutFields().isEmpty()) {
            request.getOutFields().forEach(builder::addOutputFields);
        }

        if (request.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }

        return builder.build();
    }

    public String getExprById(String primaryFieldName, List<?> ids) {
        StringBuilder sb = new StringBuilder();
        sb.append(primaryFieldName).append(" in [");
        for (Object id : ids) {
            if (id instanceof String) {
                sb.append("\"").append(id.toString()).append("\",");
            } else {
                sb.append(id.toString()).append(",");
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }
}

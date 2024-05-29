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
import io.milvus.param.ParamUtils;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;

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

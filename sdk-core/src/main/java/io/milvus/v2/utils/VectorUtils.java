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

import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.google.protobuf.ByteString;
import io.milvus.common.utils.GTsDict;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.exception.ParamException;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.request.ranker.WeightedRanker;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class VectorUtils {

    public QueryRequest ConvertToGrpcQueryRequest(QueryReq request){
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        long guaranteeTimestamp = getGuaranteeTimestamp(request.getConsistencyLevel(), dbName, collectionName);
        QueryRequest.Builder builder = QueryRequest.newBuilder()
                .setCollectionName(collectionName)
                .addAllPartitionNames(request.getPartitionNames())
                .addAllOutputFields(request.getOutputFields())
                .setGuaranteeTimestamp(guaranteeTimestamp)
                .setExpr(request.getFilter());
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        if (StringUtils.isNotEmpty(request.getFilter())) {
            Map<String, Object> filterTemplateValues = request.getFilterTemplateValues();
            filterTemplateValues.forEach((key, value)->{
                builder.putExprTemplateValues(key, deduceAndCreateTemplateValue(value));
            });
        }

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
        builder.addQueryParams(KeyValuePair.newBuilder()
                .setKey(Constant.IGNORE_GROWING)
                .setValue(String.valueOf(request.isIgnoreGrowing()))
                .build());

        return builder.build();

    }

    private static long getGuaranteeTimestamp(ConsistencyLevel consistencyLevel, String dbName, String collectionName){
        if(consistencyLevel == null){
            String key = GTsDict.CombineCollectionName(dbName, collectionName);
            Long ts = GTsDict.getInstance().getCollectionTs(key);
            return  (ts == null) ? 1L : ts;
        }
        switch (consistencyLevel){
            case STRONG:
                return 0L;
            case SESSION: {
                String key = GTsDict.CombineCollectionName(dbName, collectionName);
                Long ts = GTsDict.getInstance().getCollectionTs(key);
                return (ts == null) ? 1L : ts;
            }
            case BOUNDED:
                return 2L; // let server side to determine the bounded time
            default:
                return 1L; // EVENTUALLY and others
        }
    }

    private static ByteString convertPlaceholder(List<Object> data, PlaceholderType placeType) {
        if (placeType == PlaceholderType.VarChar) {
            List<ByteString> byteStrings = new ArrayList<>();
            for (Object obj : data) {
                byteStrings.add(ByteString.copyFrom(((String)obj).getBytes()));
            }
            PlaceholderValue.Builder pldBuilder = PlaceholderValue.newBuilder()
                    .setTag(Constant.VECTOR_TAG)
                    .setType(placeType);
            byteStrings.forEach(pldBuilder::addValues);

            PlaceholderValue plv = pldBuilder.build();
            PlaceholderGroup placeholderGroup = PlaceholderGroup.newBuilder()
                    .addPlaceholders(plv)
                    .build();
            return placeholderGroup.toByteString();
        } else {
            try {
                return ParamUtils.convertPlaceholder(data, placeType);
            } catch (ParamException e) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS, e.getMessage());
            }
        }
    }

    public SearchRequest ConvertToGrpcSearchRequest(SearchReq request) {
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        SearchRequest.Builder builder = SearchRequest.newBuilder()
                .setCollectionName(collectionName);
        if (!request.getPartitionNames().isEmpty()) {
            request.getPartitionNames().forEach(builder::addPartitionNames);
        }

        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        // prepare target, the input could be vectors or string list for doc-in-doc-out
        List<BaseVector> vectors = request.getData();
        if (vectors.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Target data list of search request is empty.");
        }

        // the elements must be all-vector or all-string
        PlaceholderType plType = vectors.get(0).getPlaceholderType();
        List<Object> data = new ArrayList<>();
        for (BaseVector vector : vectors) {
            if (vector.getPlaceholderType() != plType) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Different types of target vectors in a search request is not allowed.");
            }
            data.add(vector.getData());
        }

        ByteString byteStr = convertPlaceholder(data, plType);
        builder.setPlaceholderGroup(byteStr);
        builder.setNq(vectors.size());

        // search parameters
        // tries to fit the compatibility between v2.5.1 and older versions
        Map<String, Object> searchParams = request.getSearchParams();
        ParamUtils.compatibleSearchParams(searchParams, builder);

        // the following parameters are not changed
        // just note: if the searchParams already contains the same key, the following parameters will overwrite it
        if (StringUtils.isNotEmpty(request.getAnnsField())) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.VECTOR_FIELD)
                            .setValue(request.getAnnsField())
                            .build());
        }

        // topK value is deprecated, always use "limit" to set the topK
        builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(request.getLimit()))
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

        if (null != request.getMetricType()) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.METRIC_TYPE)
                            .setValue(request.getMetricType().name())
                            .build());
        }

        if (request.getGroupByFieldName() != null && !request.getGroupByFieldName().isEmpty()) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.GROUP_BY_FIELD)
                            .setValue(request.getGroupByFieldName())
                            .build());
            if (request.getGroupSize() != null) {
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.GROUP_SIZE)
                                .setValue(request.getGroupSize().toString())
                                .build());
            }

            if (request.getStrictGroupSize() != null) {
                builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.STRICT_GROUP_SIZE)
                                .setValue(request.getStrictGroupSize().toString())
                                .build());
            }
        }

        if (!request.getOutputFields().isEmpty()) {
            request.getOutputFields().forEach(builder::addOutputFields);
        }

        // always use expression since dsl is discarded
        builder.setDslType(DslType.BoolExprV1);
        if (request.getFilter() != null && !request.getFilter().isEmpty()) {
            builder.setDsl(request.getFilter());
            Map<String, Object> filterTemplateValues = request.getFilterTemplateValues();
            filterTemplateValues.forEach((key, value)->{
                builder.putExprTemplateValues(key, deduceAndCreateTemplateValue(value));
            });
        }

        // the SearchIteratorV2 passes a guaranteeTimestamp value, no need to call getGuaranteeTimestamp()
        if (request.getSearchParams().containsKey("iterator")) {
            long guaranteeTimestamp = 0;
            if (request.getSearchParams().containsKey("guarantee_timestamp")) {
                guaranteeTimestamp = (long)request.getSearchParams().get("guarantee_timestamp");
            }
            builder.setGuaranteeTimestamp(guaranteeTimestamp);
        } else {
            long guaranteeTimestamp = getGuaranteeTimestamp(request.getConsistencyLevel(), dbName, collectionName);
            builder.setGuaranteeTimestamp(guaranteeTimestamp);
        }

        // a new parameter from v2.2.9, if user didn't specify consistency level, set this parameter to true
        if (request.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }

        // set ranker, support reranking search result from v2.6.1
        CreateCollectionReq.Function ranker = request.getRanker();
        io.milvus.v2.service.vector.request.FunctionScore functionScore = request.getFunctionScore();
        if (ranker != null && functionScore != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Not allow to set both ranker and functionScore.");
        }
        if (functionScore != null) {
            builder.setFunctionScore(convertFunctionScore(functionScore));
        } else if (ranker != null) {
            builder.setFunctionScore(convertOneFunction(ranker));
        }

        return builder.build();
    }

    private static TemplateArrayValue deduceTemplateArray(List<?> array) {
        if (array.isEmpty()) {
            return TemplateArrayValue.newBuilder().build(); // an empty list?
        }

        Object firstObj = array.get(0);
        if (firstObj instanceof Boolean) {
            BoolArray.Builder builder = BoolArray.newBuilder();
            array.forEach(val->{
                if (!(val instanceof Boolean)) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "Filter expression template is a list, the first value is Boolean, but some elements are not Boolean");
                }
                builder.addData((Boolean)val);
            });
            return TemplateArrayValue.newBuilder().setBoolData(builder.build()).build();
        } else if (firstObj instanceof Integer || firstObj instanceof Long) {
            LongArray.Builder builder = LongArray.newBuilder();
            array.forEach(val->{
                if (!(val instanceof Integer) && !(val instanceof Long)) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "Filter expression template is a list, the first value is Integer/Long, but some elements are not Integer/Long");
                }
                builder.addData((val instanceof Integer) ? (Integer)val : (Long)val);
            });
            return TemplateArrayValue.newBuilder().setLongData(builder.build()).build();
        } else if (firstObj instanceof Double) {
            DoubleArray.Builder builder = DoubleArray.newBuilder();
            array.forEach(val->{
                if (!(val instanceof Double)) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "Filter expression template is a list, the first value is Double, but some elements are not Double");
                }
                builder.addData((Double)val);
            });
            return TemplateArrayValue.newBuilder().setDoubleData(builder.build()).build();
        } else if (firstObj instanceof String) {
            StringArray.Builder builder = StringArray.newBuilder();
            array.forEach(val->{
                if (!(val instanceof String)) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "Filter expression template is a list, the first value is String, but some elements are not String");
                }
                builder.addData((String)val);
            });
            return TemplateArrayValue.newBuilder().setStringData(builder.build()).build();
        } else if (firstObj instanceof JsonElement) {
            JSONArray.Builder builder = JSONArray.newBuilder();
            array.forEach(val->{
                if (!(val instanceof JsonElement)) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "Filter expression template is a list, the first value is JsonElement, but some elements are not JsonElement");
                }
                String str = JsonUtils.toJson((JsonElement)val);
                builder.addData(ByteString.copyFromUtf8(str));
            });
            return TemplateArrayValue.newBuilder().setJsonData(builder.build()).build();
        } else if (firstObj instanceof List) {
            TemplateArrayValueArray.Builder builder = TemplateArrayValueArray.newBuilder();
            array.forEach(val->{
                if (!(val instanceof List)) {
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                            "Filter expression template is a list, the first value is List, but some elements are not List");
                }
                List<?> subArrary = (List<?>)val;
                builder.addData(deduceTemplateArray(subArrary));
            });

            return TemplateArrayValue.newBuilder().setArrayData(builder.build()).build();
        } else {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Unsupported value type for filter expression template.");
        }
    }

    public static TemplateValue deduceAndCreateTemplateValue(Object value) {
        if (value instanceof Boolean) {
            return TemplateValue.newBuilder()
                    .setBoolVal((Boolean)value)
                    .build();
        } else if (value instanceof Integer || value instanceof Long) {
           return TemplateValue.newBuilder()
                   .setInt64Val((value instanceof Integer) ? (Integer)value : (Long)value)
                   .build();
        } else if (value instanceof Double) {
            return TemplateValue.newBuilder()
                    .setFloatVal((Double)value)
                    .build();
        } else if (value instanceof String) {
            return TemplateValue.newBuilder()
                    .setStringVal((String)value)
                    .build();
        } else if (value instanceof List) {
            List<?> array = (List<?>)value;
            TemplateArrayValue tav = deduceTemplateArray(array);
            return TemplateValue.newBuilder()
                    .setArrayVal(tav)
                    .build();
        } else {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Unsupported value type for filter expression template.");
        }
    }

    public static SearchRequest convertAnnSearchParam(@NonNull AnnSearchReq annSearchReq,
                                                      ConsistencyLevel consistencyLevel) {
        SearchRequest.Builder builder = SearchRequest.newBuilder();
        // prepare target vectors
        List<BaseVector> vectors = annSearchReq.getVectors();
        if (vectors.isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                    "Target vectors list of search request is empty.");
        }
        PlaceholderType plType = vectors.get(0).getPlaceholderType();
        List<Object> data = new ArrayList<>();
        for (BaseVector vector : vectors) {
            if (vector.getPlaceholderType() != plType) {
                throw new MilvusClientException(ErrorCode.INVALID_PARAMS,
                        "Different types of target vectors in a search request is not allowed.");
            }
            data.add(vector.getData());
        }

        ByteString byteStr = convertPlaceholder(data, plType);
        builder.setPlaceholderGroup(byteStr);
        builder.setNq(vectors.size());

        // search parameters
        // tries to fit the compatibility between v2.5.1 and older versions
        Map<String, Object> paramMap = new HashMap<>();
        if (null != annSearchReq.getParams() && !annSearchReq.getParams().isEmpty()) {
            paramMap = JsonUtils.fromJson(annSearchReq.getParams(), new TypeToken<Map<String, Object>>() {}.getType());
        }
        ParamUtils.compatibleSearchParams(paramMap, builder);

        builder.addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.VECTOR_FIELD)
                                .setValue(annSearchReq.getVectorFieldName())
                                .build())
                .addSearchParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.TOP_K)
                                .setValue(String.valueOf(annSearchReq.getLimit()))
                                .build());
        if (annSearchReq.getMetricType() != null) {
            builder.addSearchParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.METRIC_TYPE)
                            .setValue(annSearchReq.getMetricType().name())
                            .build());
        }

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
        String dbName = request.getDatabaseName();
        String collectionName = request.getCollectionName();
        HybridSearchRequest.Builder builder = HybridSearchRequest.newBuilder()
                .setCollectionName(collectionName);

        if (request.getPartitionNames() != null && !request.getPartitionNames().isEmpty()) {
            request.getPartitionNames().forEach(builder::addPartitionNames);
        }
        if (StringUtils.isNotEmpty(dbName)) {
            builder.setDbName(dbName);
        }

        if (request.getSearchRequests() == null || request.getSearchRequests().isEmpty()) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Sub-request list is empty.");
        }

        for (AnnSearchReq req : request.getSearchRequests()) {
            SearchRequest searchRequest = convertAnnSearchParam(req, request.getConsistencyLevel());
            builder.addRequests(searchRequest);
        }

        Map<String, String> props = new HashMap<>();
        props.put(Constant.LIMIT, String.valueOf(request.getLimit()));
        props.put(Constant.ROUND_DECIMAL, String.valueOf(request.getRoundDecimal()));
        props.put(Constant.OFFSET, String.valueOf(request.getOffset()));

        // set ranker
        CreateCollectionReq.Function ranker = request.getRanker();
        io.milvus.v2.service.vector.request.FunctionScore functionScore = request.getFunctionScore();
        if (ranker != null && functionScore != null) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "Not allow to set both ranker and functionScore.");
        }
        if (functionScore != null) {
            builder.setFunctionScore(convertFunctionScore(functionScore));
        } else if (ranker != null) {
            if (ranker instanceof RRFRanker || ranker instanceof WeightedRanker) {
                // old logic for RRF/Weighted ranker
                Map<String, String> params = ranker.getParams();
                props.putAll(params);
            } else {
                // new logic for Decay/Model ranker
                builder.setFunctionScore(convertOneFunction(ranker));
            }
        }

        List<KeyValuePair> propertiesList = ParamUtils.AssembleKvPair(props);
        if (CollectionUtils.isNotEmpty(propertiesList)) {
            propertiesList.forEach(builder::addRankParams);
        }

        if (request.getGroupByFieldName() != null && !request.getGroupByFieldName().isEmpty()) {
            builder.addRankParams(
                    KeyValuePair.newBuilder()
                            .setKey(Constant.GROUP_BY_FIELD)
                            .setValue(request.getGroupByFieldName())
                            .build());
            if (request.getGroupSize() != null) {
                builder.addRankParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.GROUP_SIZE)
                                .setValue(request.getGroupSize().toString().toString())
                                .build());
            }

            if (request.getStrictGroupSize() != null) {
                builder.addRankParams(
                        KeyValuePair.newBuilder()
                                .setKey(Constant.STRICT_GROUP_SIZE)
                                .setValue(request.getStrictGroupSize().toString())
                                .build());
            }
        }

        // output fields
        if (request.getOutFields() != null && !request.getOutFields().isEmpty()) {
            request.getOutFields().forEach(builder::addOutputFields);
        }

        long guaranteeTimestamp = getGuaranteeTimestamp(request.getConsistencyLevel(), dbName, collectionName);
        builder.setGuaranteeTimestamp(guaranteeTimestamp);

        if (request.getConsistencyLevel() == null) {
            builder.setUseDefaultConsistency(true);
        } else {
            builder.setConsistencyLevelValue(request.getConsistencyLevel().getCode());
        }

        return builder.build();
    }

    private FunctionSchema convertFunctionSchema(CreateCollectionReq.Function function) {
        Map<String, String> params = function.getParams();
        // FunctionSchema type keyword is "reranker"
        // old RRF/Weighted ranker type keyword is "strategy"
        if (function instanceof RRFRanker) {
            params.put("reranker", "rrf");
            params.remove("strategy");
        } else if (function instanceof WeightedRanker) {
            params.put("reranker", "weighted");
            params.remove("strategy");
        }
        return FunctionSchema.newBuilder()
                .setName(function.getName())
                .setDescription(function.getDescription())
                .setType(FunctionType.forNumber(function.getFunctionType().getCode()))
                .addAllInputFieldNames(function.getInputFieldNames())
                .addAllParams(ParamUtils.AssembleKvPair(function.getParams()))
                .build();
    }

    private io.milvus.grpc.FunctionScore convertOneFunction(CreateCollectionReq.Function function) {
        FunctionSchema schema = convertFunctionSchema(function);
        return io.milvus.grpc.FunctionScore.newBuilder().addFunctions(schema).build();
    }

    private io.milvus.grpc.FunctionScore convertFunctionScore(FunctionScore functionScore) {
        io.milvus.grpc.FunctionScore.Builder builder = io.milvus.grpc.FunctionScore.newBuilder();
        for (CreateCollectionReq.Function function : functionScore.getFunctions()) {
            FunctionSchema schema = convertFunctionSchema(function);
            builder.addFunctions(schema);
        }
        builder.addAllParams(ParamUtils.AssembleKvPair(functionScore.getParams()));
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

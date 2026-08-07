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

import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.FunctionScore;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class VectorRequestCopier {
    private VectorRequestCopier() {
    }

    static QueryReq copy(QueryReq request) {
        return QueryReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(request.getClusterId())
                .partitionNames(copyList(request.getPartitionNames()))
                .outputFields(copyList(request.getOutputFields()))
                .ids(copyList(request.getIds()))
                .filter(request.getFilter())
                .consistencyLevel(request.getConsistencyLevel())
                .offset(request.getOffset())
                .limit(request.getLimit())
                .ignoreGrowing(request.isIgnoreGrowing())
                .timezone(request.getTimezone())
                .orderByFields(copyList(request.getOrderByFields()))
                .queryParams(copyMap(request.getQueryParams()))
                .filterTemplateValues(copyMap(request.getFilterTemplateValues()))
                .build();
    }

    static SearchReq copy(SearchReq request) {
        return SearchReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(request.getClusterId())
                .partitionNames(copyList(request.getPartitionNames()))
                .annsField(request.getAnnsField())
                .metricType(request.getMetricType())
                .filter(request.getFilter())
                .outputFields(copyList(request.getOutputFields()))
                .data(copyList(request.getData()))
                .ids(copyList(request.getIds()))
                .offset(request.getOffset())
                .limit(request.getLimit())
                .roundDecimal(request.getRoundDecimal())
                .searchParams(copyMap(request.getSearchParams()))
                .guaranteeTimestamp(request.getGuaranteeTimestamp())
                .gracefulTime(request.getGracefulTime())
                .consistencyLevel(request.getConsistencyLevel())
                .ignoreGrowing(request.isIgnoreGrowing())
                .timezone(request.getTimezone())
                .orderByFields(copyList(request.getOrderByFields()))
                .groupByFieldName(request.getGroupByFieldName())
                .groupSize(request.getGroupSize())
                .strictGroupSize(request.getStrictGroupSize())
                .ranker(request.getRanker())
                .functionScore(copyFunctionScore(request.getFunctionScore()))
                .filterTemplateValues(copyMap(request.getFilterTemplateValues()))
                .highlighter(request.getHighlighter())
                .searchAggregation(request.getSearchAggregation())
                .build();
    }

    static HybridSearchReq copy(HybridSearchReq request) {
        List<AnnSearchReq> searchRequests = null;
        if (request.getSearchRequests() != null) {
            searchRequests = new ArrayList<>();
            for (AnnSearchReq searchRequest : request.getSearchRequests()) {
                searchRequests.add(copy(searchRequest));
            }
        }
        return HybridSearchReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(request.getClusterId())
                .partitionNames(copyList(request.getPartitionNames()))
                .searchRequests(searchRequests)
                .ranker(request.getRanker())
                .functionScore(copyFunctionScore(request.getFunctionScore()))
                .limit(request.getLimit())
                .outFields(copyList(request.getOutFields()))
                .offset(request.getOffset())
                .roundDecimal(request.getRoundDecimal())
                .consistencyLevel(request.getConsistencyLevel())
                .groupByFieldName(request.getGroupByFieldName())
                .groupSize(request.getGroupSize())
                .strictGroupSize(request.getStrictGroupSize())
                .build();
    }

    private static AnnSearchReq copy(AnnSearchReq request) {
        return AnnSearchReq.builder()
                .vectorFieldName(request.getVectorFieldName())
                .limit(request.getLimit())
                .filter(request.getFilter())
                .vectors(copyList(request.getVectors()))
                .params(request.getParams())
                .metricType(request.getMetricType())
                .timezone(request.getTimezone())
                .filterTemplateValues(copyMap(request.getFilterTemplateValues()))
                .build();
    }

    private static FunctionScore copyFunctionScore(FunctionScore functionScore) {
        if (functionScore == null) {
            return null;
        }
        return FunctionScore.builder()
                .functions(copyList(functionScore.getFunctions()))
                .params(copyMap(functionScore.getParams()))
                .build();
    }

    private static <T> List<T> copyList(List<T> source) {
        return source == null ? null : new ArrayList<>(source);
    }

    private static <K, V> Map<K, V> copyMap(Map<K, V> source) {
        return source == null ? null : new HashMap<>(source);
    }
}

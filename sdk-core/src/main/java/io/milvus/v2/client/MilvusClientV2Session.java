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
import org.apache.commons.lang3.StringUtils;

public class MilvusClientV2Session {
    private final MilvusClientV2 parent;
    private final String clusterId;
    private boolean closed = false;

    MilvusClientV2Session(MilvusClientV2 parent, String clusterId) {
        this.parent = parent;
        this.clusterId = clusterId;
    }

    public SearchResp search(SearchReq request) {
        ensureOpen();
        return parent.search(copy(request));
    }

    public SearchResp hybridSearch(HybridSearchReq request) {
        ensureOpen();
        return parent.hybridSearch(copy(request));
    }

    public QueryResp query(QueryReq request) {
        ensureOpen();
        return parent.query(copy(request));
    }

    public QueryIterator queryIterator(QueryIteratorReq request) {
        ensureOpen();
        return parent.queryIterator(copy(request));
    }

    public SearchIterator searchIterator(SearchIteratorReq request) {
        ensureOpen();
        return parent.searchIterator(copy(request));
    }

    public SearchIteratorV2 searchIteratorV2(SearchIteratorReqV2 request) {
        ensureOpen();
        return parent.searchIteratorV2(copy(request));
    }

    public GetResp get(GetReq request) {
        ensureOpen();
        return parent.get(copy(request));
    }

    public void close() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "MilvusClient session is closed");
        }
    }

    private void checkClusterId(String requestClusterId) {
        if (StringUtils.isNotEmpty(requestClusterId) && !clusterId.equals(requestClusterId)) {
            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, "clusterId conflicts with session clusterId");
        }
    }

    private SearchReq copy(SearchReq request) {
        checkClusterId(request.getClusterId());
        return SearchReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(clusterId)
                .partitionNames(request.getPartitionNames())
                .annsField(request.getAnnsField())
                .metricType(request.getMetricType())
                .filter(request.getFilter())
                .outputFields(request.getOutputFields())
                .data(request.getData())
                .ids(request.getIds())
                .offset(request.getOffset())
                .limit(request.getLimit())
                .roundDecimal(request.getRoundDecimal())
                .searchParams(request.getSearchParams())
                .guaranteeTimestamp(request.getGuaranteeTimestamp())
                .gracefulTime(request.getGracefulTime())
                .consistencyLevel(request.getConsistencyLevel())
                .ignoreGrowing(request.isIgnoreGrowing())
                .timezone(request.getTimezone())
                .groupByFieldName(request.getGroupByFieldName())
                .groupSize(request.getGroupSize())
                .strictGroupSize(request.getStrictGroupSize())
                .ranker(request.getRanker())
                .functionScore(request.getFunctionScore())
                .filterTemplateValues(request.getFilterTemplateValues())
                .highlighter(request.getHighlighter())
                .searchAggregation(request.getSearchAggregation())
                .build();
    }

    private HybridSearchReq copy(HybridSearchReq request) {
        checkClusterId(request.getClusterId());
        return HybridSearchReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(clusterId)
                .partitionNames(request.getPartitionNames())
                .searchRequests(request.getSearchRequests())
                .ranker(request.getRanker())
                .functionScore(request.getFunctionScore())
                .limit(request.getLimit())
                .outFields(request.getOutFields())
                .offset(request.getOffset())
                .roundDecimal(request.getRoundDecimal())
                .consistencyLevel(request.getConsistencyLevel())
                .groupByFieldName(request.getGroupByFieldName())
                .groupSize(request.getGroupSize())
                .strictGroupSize(request.getStrictGroupSize())
                .build();
    }

    private QueryReq copy(QueryReq request) {
        checkClusterId(request.getClusterId());
        return QueryReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(clusterId)
                .partitionNames(request.getPartitionNames())
                .outputFields(request.getOutputFields())
                .ids(request.getIds())
                .filter(request.getFilter())
                .consistencyLevel(request.getConsistencyLevel())
                .offset(request.getOffset())
                .limit(request.getLimit())
                .ignoreGrowing(request.isIgnoreGrowing())
                .timezone(request.getTimezone())
                .queryParams(request.getQueryParams())
                .filterTemplateValues(request.getFilterTemplateValues())
                .build();
    }

    private QueryIteratorReq copy(QueryIteratorReq request) {
        checkClusterId(request.getClusterId());
        return QueryIteratorReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(clusterId)
                .partitionNames(request.getPartitionNames())
                .outputFields(request.getOutputFields())
                .expr(request.getExpr())
                .consistencyLevel(request.getConsistencyLevel())
                .offset(request.getOffset())
                .limit(request.getLimit())
                .ignoreGrowing(request.isIgnoreGrowing())
                .timezone(request.getTimezone())
                .batchSize(request.getBatchSize())
                .reduceStopForBest(request.isReduceStopForBest())
                .filterTemplateValues(request.getFilterTemplateValues())
                .build();
    }

    private SearchIteratorReq copy(SearchIteratorReq request) {
        checkClusterId(request.getClusterId());
        return SearchIteratorReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(clusterId)
                .partitionNames(request.getPartitionNames())
                .metricType(request.getMetricType())
                .vectorFieldName(request.getVectorFieldName())
                .limit(request.getLimit())
                .expr(request.getExpr())
                .outputFields(request.getOutputFields())
                .vectors(request.getVectors())
                .roundDecimal(request.getRoundDecimal())
                .params(request.getParams())
                .consistencyLevel(request.getConsistencyLevel())
                .ignoreGrowing(request.isIgnoreGrowing())
                .groupByFieldName(request.getGroupByFieldName())
                .batchSize(request.getBatchSize())
                .build();
    }

    private SearchIteratorReqV2 copy(SearchIteratorReqV2 request) {
        checkClusterId(request.getClusterId());
        return SearchIteratorReqV2.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(clusterId)
                .partitionNames(request.getPartitionNames())
                .metricType(request.getMetricType())
                .vectorFieldName(request.getVectorFieldName())
                .limit(request.getLimit())
                .filter(request.getFilter())
                .outputFields(request.getOutputFields())
                .vectors(request.getVectors())
                .roundDecimal(request.getRoundDecimal())
                .searchParams(request.getSearchParams())
                .consistencyLevel(request.getConsistencyLevel())
                .ignoreGrowing(request.isIgnoreGrowing())
                .timezone(request.getTimezone())
                .groupByFieldName(request.getGroupByFieldName())
                .batchSize(request.getBatchSize())
                .externalFilterFunc(request.getExternalFilterFunc())
                .filterTemplateValues(request.getFilterTemplateValues())
                .build();
    }

    private GetReq copy(GetReq request) {
        checkClusterId(request.getClusterId());
        return GetReq.builder()
                .databaseName(request.getDatabaseName())
                .collectionName(request.getCollectionName())
                .clusterId(clusterId)
                .partitionName(request.getPartitionName())
                .ids(request.getIds())
                .outputFields(request.getOutputFields())
                .build();
    }
}

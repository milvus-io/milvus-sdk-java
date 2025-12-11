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

package io.milvus.orm.iterator;

import io.milvus.grpc.DataType;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.QueryResults;
import io.milvus.param.Constant;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.utils.RpcUtils;
import io.milvus.v2.utils.VectorUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.milvus.param.Constant.*;

public class QueryIterator {
    protected static final Logger logger = LoggerFactory.getLogger(RpcUtils.class);
    private final IteratorCache iteratorCache;
    private final RpcStubWrapper blockingStub;
    private final FieldType primaryField;

    private final QueryIteratorReq queryIteratorReq;
    private final int batchSize;
    private final long limit;
    private final String expr;
    private long offset;
    private Object nextId;
    private int cacheIdInUse;
    private long returnedCount;
    private final RpcUtils rpcUtils;
    private long sessionTs = 0;

    public QueryIterator(QueryIteratorParam queryIteratorParam,
                         RpcStubWrapper blockingStub,
                         FieldType primaryField) {
        this.iteratorCache = new IteratorCache();
        this.blockingStub = blockingStub;
        this.primaryField = primaryField;
        this.queryIteratorReq = IteratorAdapterV2.convertV1Param(queryIteratorParam);

        this.batchSize = (int) queryIteratorParam.getBatchSize();
        this.expr = queryIteratorParam.getExpr();
        this.limit = queryIteratorParam.getLimit();
        this.offset = queryIteratorParam.getOffset();
        this.rpcUtils = new RpcUtils();

        setupTsByRequest();
        seek();
    }

    public QueryIterator(QueryIteratorReq queryIteratorReq,
                         RpcStubWrapper blockingStub,
                         CreateCollectionReq.FieldSchema primaryField) {
        this.iteratorCache = new IteratorCache();
        this.blockingStub = blockingStub;
        this.queryIteratorReq = queryIteratorReq;
        this.primaryField = IteratorAdapterV2.convertV2Field(primaryField);

        this.batchSize = (int) queryIteratorReq.getBatchSize();
        this.expr = queryIteratorReq.getExpr();
        this.limit = queryIteratorReq.getLimit();
        this.offset = queryIteratorReq.getOffset();
        this.rpcUtils = new RpcUtils();

        setupTsByRequest();
        seek();
    }

    // perform a query to get the first time stamp check point
    // the time stamp will be input for the next query to skip something
    private void setupTsByRequest() {
        QueryResults response = executeQuery(expr, 0L, 1L, 0L, true);
        if (response.getSessionTs() <= 0) {
            logger.warn("Failed to get mvccTs from milvus server, use client-side ts instead");
            // fall back to latest session ts by local time
            long ts = System.currentTimeMillis() + 1000L;
            this.sessionTs = ts << 18;
        } else {
            this.sessionTs = response.getSessionTs();
        }
    }

    private void seek() {
        this.cacheIdInUse = NO_CACHE_ID;
        if (offset == 0) {
            nextId = null;
            return;
        }

        long currentOffset = offset;
        while (currentOffset > 0) {
            long limit = Math.min(MAX_BATCH_SIZE, currentOffset);
            String currentExpr = setupNextExpr();
            QueryResults response = executeQuery(currentExpr, 0L, limit, this.sessionTs, true);
            QueryResultsWrapper queryWrapper = new QueryResultsWrapper(response);
            List<QueryResultsWrapper.RowRecord> res = queryWrapper.getRowRecords();
            if (res.isEmpty()) {
                break;
            }
            updateCursor(res);
            currentOffset -= res.size();
        }
        offset = 0;
    }

    public List<QueryResultsWrapper.RowRecord> next() {
        List<QueryResultsWrapper.RowRecord> cachedRes = iteratorCache.fetchCache(cacheIdInUse);
        List<QueryResultsWrapper.RowRecord> ret;
        if (isResSufficient(cachedRes)) {
            ret = cachedRes.subList(0, batchSize);
            List<QueryResultsWrapper.RowRecord> retToCache = cachedRes.subList(batchSize, cachedRes.size());
            iteratorCache.cache(cacheIdInUse, retToCache);
        } else {
            iteratorCache.releaseCache(cacheIdInUse);
            String currentExpr = setupNextExpr();
            logger.debug("Query iterator next expression: " + currentExpr);
            QueryResults response = executeQuery(currentExpr, offset, batchSize, this.sessionTs, false);
            QueryResultsWrapper queryWrapper = new QueryResultsWrapper(response);
            List<QueryResultsWrapper.RowRecord> res = queryWrapper.getRowRecords();
            maybeCache(res);
            ret = res.subList(0, Math.min(batchSize, res.size()));
        }
        ret = checkReachedLimit(ret);
        updateCursor(ret);
        returnedCount += ret.size();
        return ret;
    }

    public void close() {
        iteratorCache.releaseCache(cacheIdInUse);
    }

    private void updateCursor(List<QueryResultsWrapper.RowRecord> res) {
        if (res.isEmpty()) {
            return;
        }
        nextId = res.get(res.size() - 1).get(primaryField.getName());
    }

    private List<QueryResultsWrapper.RowRecord> checkReachedLimit(List<QueryResultsWrapper.RowRecord> ret) {
        if (limit == UNLIMITED) {
            return ret;
        }
        long leftCount = limit - returnedCount;
        if (leftCount >= ret.size()) {
            return ret;
        }

        return ret.subList(0, (int) leftCount);
    }

    private void maybeCache(List<QueryResultsWrapper.RowRecord> ret) {
        if (ret.size() < 2 * batchSize) {
            return;
        }
        List<QueryResultsWrapper.RowRecord> cacheResult = ret.subList(batchSize, ret.size());
        cacheIdInUse = iteratorCache.cache(NO_CACHE_ID, cacheResult);
    }

    private String setupNextExpr() {
        String currentExpr = expr;
        if (nextId == null) {
            return currentExpr;
        }
        String filteredPKStr;
        if (primaryField.getDataType() == DataType.VarChar) {
            filteredPKStr = primaryField.getName() + " > " + "\"" + nextId + "\"";
        } else {
            filteredPKStr = primaryField.getName() + " > " + nextId;
        }
        if (StringUtils.isEmpty(currentExpr)) {
            return filteredPKStr;
        }
        return " ( " + currentExpr + " ) " + " and " + filteredPKStr;
    }

    private boolean isResSufficient(List<QueryResultsWrapper.RowRecord> ret) {
        return ret != null && ret.size() >= batchSize;
    }

    private QueryResults executeQuery(String expr, long offset, long limit, long ts, boolean isSeek) {
        // for seeking offset, no need to return output fields
        List<String> outputFields = new ArrayList<>();
        boolean reduceStopForBest = queryIteratorReq.isReduceStopForBest();
        if (!isSeek) {
            outputFields = queryIteratorReq.getOutputFields();
            reduceStopForBest = false;
        }
        QueryReq queryReq = QueryReq.builder()
                .databaseName(queryIteratorReq.getDatabaseName())
                .collectionName(queryIteratorReq.getCollectionName())
                .partitionNames(queryIteratorReq.getPartitionNames())
                .consistencyLevel(queryIteratorReq.getConsistencyLevel())
                .outputFields(outputFields)
                .filter(expr)
                .offset(offset)
                .limit(limit)
                .ignoreGrowing(queryIteratorReq.isIgnoreGrowing())
                .timezone(queryIteratorReq.getTimezone())
                .filterTemplateValues(queryIteratorReq.getFilterTemplateValues())
                .build();

        VectorUtils vectorUtils = new VectorUtils();
        QueryRequest queryRequest = vectorUtils.ConvertToGrpcQueryRequest(queryReq);
        QueryRequest.Builder builder = queryRequest.toBuilder();
        // reduce stop for best
        builder.addQueryParams(KeyValuePair.newBuilder()
                .setKey(Constant.REDUCE_STOP_FOR_BEST)
                .setValue(String.valueOf(reduceStopForBest))
                .build());

        // iterator
        builder.addQueryParams(KeyValuePair.newBuilder()
                .setKey(Constant.ITERATOR_FIELD)
                .setValue(String.valueOf(Boolean.TRUE))
                .build());

        // pass the session ts to query interface
        builder.setGuaranteeTimestamp(ts).build();

        // set default consistency level
        builder.setUseDefaultConsistency(true);

        QueryResults response = rpcUtils.retry(() -> blockingStub.get().query(builder.build()));
        String title = String.format("QueryRequest collectionName:%s", queryIteratorReq.getCollectionName());
        rpcUtils.handleResponse(title, response.getStatus());
        return response;
    }
}

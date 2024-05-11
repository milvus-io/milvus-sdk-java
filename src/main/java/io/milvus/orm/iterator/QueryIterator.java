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
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.QueryRequest;
import io.milvus.grpc.QueryResults;
import io.milvus.param.ParamUtils;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.utils.RpcUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

import static io.milvus.param.Constant.NO_CACHE_ID;
import static io.milvus.param.Constant.UNLIMITED;

public class QueryIterator {
    private final IteratorCache iteratorCache;
    private final MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub;
    private final FieldType primaryField;

    private final QueryIteratorParam queryIteratorParam;
    private final int batchSize;
    private final long limit;
    private final String expr;
    private long offset;
    private Object nextId;
    private int cacheIdInUse;
    private long returnedCount;
    private final RpcUtils rpcUtils;

    public QueryIterator(QueryIteratorParam queryIteratorParam,
                         MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub,
                         FieldType primaryField) {
        this.iteratorCache = new IteratorCache();
        this.blockingStub = blockingStub;
        this.primaryField = primaryField;
        this.queryIteratorParam = queryIteratorParam;

        this.batchSize = (int) queryIteratorParam.getBatchSize();
        this.expr = queryIteratorParam.getExpr();
        this.limit = queryIteratorParam.getLimit();
        this.offset = queryIteratorParam.getOffset();
        this.rpcUtils = new RpcUtils();

        seek();
    }

    private void seek() {
        this.cacheIdInUse = NO_CACHE_ID;
        if (offset == 0) {
            nextId = null;
            return;
        }

        List<QueryResultsWrapper.RowRecord> res = getQueryResultsWrapper(expr, 0L, offset);
        updateCursor(res.subList(0, (int) offset));
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
            List<QueryResultsWrapper.RowRecord> res = getQueryResultsWrapper(currentExpr, offset, batchSize);
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
        return currentExpr + " and " + filteredPKStr;
    }

    private boolean isResSufficient(List<QueryResultsWrapper.RowRecord> ret) {
        return ret != null && ret.size() >= batchSize;
    }

    private List<QueryResultsWrapper.RowRecord> getQueryResultsWrapper(String expr, long offset, long limit) {
        QueryParam queryParam = QueryParam.newBuilder()
                .withDatabaseName(queryIteratorParam.getDatabaseName())
                .withCollectionName(queryIteratorParam.getCollectionName())
                .withConsistencyLevel(queryIteratorParam.getConsistencyLevel())
                .withPartitionNames(queryIteratorParam.getPartitionNames())
                .withOutFields(queryIteratorParam.getOutFields())
                .withExpr(expr)
                .withOffset(offset)
                .withLimit(limit)
                .withIgnoreGrowing(queryIteratorParam.isIgnoreGrowing())
                .build();

        QueryRequest queryRequest = ParamUtils.convertQueryParam(queryParam);
        QueryResults response = blockingStub.query(queryRequest);

        String title = String.format("QueryRequest collectionName:%s", queryIteratorParam.getCollectionName());
        rpcUtils.handleResponse(title, response.getStatus());

        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(response);
        return queryWrapper.getRowRecords();
    }
}

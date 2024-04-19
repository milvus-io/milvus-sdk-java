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

package io.milvus;

import com.google.common.collect.Lists;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.FlushResponse;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RetryParam;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryIteratorParam;
import io.milvus.param.dml.SearchIteratorParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.response.GetCollStatResponseWrapper;
import io.milvus.response.QueryResultsWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IteratorExample {
    private static final MilvusClient milvusClient;

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        RetryParam retryParam = RetryParam.newBuilder()
                .withMaxRetryTimes(3)
                .build();
        milvusClient = new MilvusServiceClient(connectParam).withRetry(retryParam);
    }

    private static final String COLLECTION_NAME = "test_iterator";
    private static final String ID_FIELD = "userID";
    private static final String VECTOR_FIELD = "userFace";
    private static final Integer VECTOR_DIM = 8;
    private static final String AGE_FIELD = "userAge";

    private static final String INDEX_NAME = "userFaceIndex";
    private static final IndexType INDEX_TYPE = IndexType.IVF_FLAT;
    private static final String INDEX_PARAM = "{\"nlist\":128}";
    private static final boolean CLEAR_EXIST = false;
    private static final Integer NUM_ENTITIES = 1000;

    private void createCollection(long timeoutMilliseconds) {
        FieldType fieldType1 = FieldType.newBuilder()
                .withName(ID_FIELD)
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(false)
                .build();

        FieldType fieldType2 = FieldType.newBuilder()
                .withName(VECTOR_FIELD)
                .withDataType(DataType.FloatVector)
                .withDimension(VECTOR_DIM)
                .build();

        FieldType fieldType3 = FieldType.newBuilder()
                .withName(AGE_FIELD)
                .withDataType(DataType.Int64)
                .build();

        CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(false)
                .addFieldType(fieldType1)
                .addFieldType(fieldType2)
                .addFieldType(fieldType3)
                .build();

        CreateCollectionParam createCollectionReq = CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withShardsNum(2)
                .withSchema(collectionSchemaParam)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .build();
        R<RpcStatus> response = milvusClient.withTimeout(timeoutMilliseconds, TimeUnit.MILLISECONDS)
                .createCollection(createCollectionReq);
        CommonUtils.handleResponseStatus(response);
    }

    private boolean hasCollection() {
        R<Boolean> response = milvusClient.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        CommonUtils.handleResponseStatus(response);
        return response.getData();
    }

    private void dropCollection() {
        R<RpcStatus> response = milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        CommonUtils.handleResponseStatus(response);
    }

    private void loadCollection() {
        R<RpcStatus> response = milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        CommonUtils.handleResponseStatus(response);
        System.out.printf("Finish Loading Collection %s\n", COLLECTION_NAME);
    }

    private void createIndex() {
        // create index for vector field
        R<RpcStatus> response = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexName(INDEX_NAME)
                .withIndexType(INDEX_TYPE)
                .withMetricType(MetricType.L2)
                .withExtraParam(INDEX_PARAM)
                .withSyncMode(Boolean.TRUE)
                .build());
        CommonUtils.handleResponseStatus(response);
        System.out.printf("Finish Creating index %s\n", INDEX_TYPE);
    }

    private void insertColumns() {
        int batchCount = 5;
        for (int batch = 0; batch < batchCount; ++batch) {
            List<List<Float>> vectors = CommonUtils.generateFixFloatVectors(VECTOR_DIM, NUM_ENTITIES);

            List<Long> ages = new ArrayList<>();
            List<Long> ids = new ArrayList<>();
            for (long i = 0L; i < NUM_ENTITIES; ++i) {
                ages.add((long) batch * NUM_ENTITIES + i);
                ids.add((long) batch * NUM_ENTITIES + i);
            }

            List<InsertParam.Field> fields = new ArrayList<>();
            fields.add(new InsertParam.Field(ID_FIELD, ids));
            fields.add(new InsertParam.Field(AGE_FIELD, ages));
            fields.add(new InsertParam.Field(VECTOR_FIELD, vectors));

            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .withFields(fields)
                    .build();
            R<MutationResult> response = milvusClient.insert(insertParam);
            CommonUtils.handleResponseStatus(response);

            R<FlushResponse> flush = milvusClient.flush(FlushParam.newBuilder().addCollectionName(COLLECTION_NAME).build());
            CommonUtils.handleResponseStatus(flush);

            GetCollectionStatisticsParam collectionStatisticsParam = GetCollectionStatisticsParam.newBuilder().withCollectionName(COLLECTION_NAME).build();
            R<GetCollectionStatisticsResponse> collectionStatistics = milvusClient.getCollectionStatistics(collectionStatisticsParam);
            CommonUtils.handleResponseStatus(collectionStatistics);
            GetCollStatResponseWrapper wrapper = new GetCollStatResponseWrapper(collectionStatistics.getData());

            System.out.printf("Finish insert batch%s, number of entities in Milvus: %s\n", batch, wrapper.getRowCount());
        }

    }

    private void reCreateCollection() {
        if (hasCollection()) {
            if (CLEAR_EXIST) {
                dropCollection();
                System.out.printf("Dropped existed collection %s%n", COLLECTION_NAME);
            }
        } else {
            createCollection(2000);
            System.out.printf("Create collection %s%n", COLLECTION_NAME);
        }
    }

    private void prepareData() {
        insertColumns();
        createIndex();
        loadCollection();
    }

    private void queryIterateCollectionNoOffset() {
        String expr = String.format("10 <= %s <= 100", AGE_FIELD);

        QueryIterator queryIterator = getQueryIterator(expr, 0L, 5L, null);
        iterateQueryResult(queryIterator);
    }

    private void queryIterateCollectionWithOffset() {
        String expr = String.format("10 <= %s <= 100", AGE_FIELD);
        QueryIterator queryIterator = getQueryIterator(expr, 10L, 50L, null);
        iterateQueryResult(queryIterator);
    }

    private void queryIterateCollectionWithLimit() {
        String expr = String.format("10 <= %s <= 100", AGE_FIELD);
        QueryIterator queryIterator = getQueryIterator(expr, null, 80L, 530L);
        iterateQueryResult(queryIterator);
    }

    private void searchIteratorCollection() {
        List<List<Float>> floatVector = CommonUtils.generateFixFloatVectors(VECTOR_DIM, 1);
        String params = buildSearchParams();
        SearchIterator searchIterator = getSearchIterator(floatVector, 500L, null, params);
        iterateSearchResult(searchIterator);
    }

    private void searchIteratorCollectionWithLimit() {
        List<List<Float>> floatVector = CommonUtils.generateFixFloatVectors(VECTOR_DIM, 1);
        String params = buildSearchParams();
        SearchIterator searchIterator = getSearchIterator(floatVector, 200L, 755, params);
        iterateSearchResult(searchIterator);
    }

    private void iterateQueryResult(QueryIterator queryIterator) {
        int pageIdx = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.println("query iteration finished, close");
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord re : res) {
                System.out.println(re);
            }
            pageIdx++;
            System.out.printf("page%s-------------------------%n", pageIdx);
        }
    }

    private void iterateSearchResult(SearchIterator searchIterator) {
        int pageIdx = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord re : res) {
                System.out.println(re);
            }
            pageIdx++;
            System.out.printf("page%s-------------------------%n", pageIdx);
        }
    }

    private QueryIterator getQueryIterator(String expr, Long offset, Long batchSize, Long limit) {
        QueryIteratorParam.Builder queryIteratorParamBuilder = QueryIteratorParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr).withOutFields(Lists.newArrayList(ID_FIELD, AGE_FIELD))
                .withBatchSize(batchSize).withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY);

        if (offset != null) {
            queryIteratorParamBuilder.withOffset(offset);
        }
        if (limit != null) {
            queryIteratorParamBuilder.withLimit(limit);
        }

        R<QueryIterator> response = milvusClient.queryIterator(queryIteratorParamBuilder.build());
        CommonUtils.handleResponseStatus(response);
        return response.getData();
    }

    private SearchIterator getSearchIterator(List<List<Float>> vectors, Long batchSize, Integer topK, String params) {
        SearchIteratorParam.Builder searchIteratorParamBuilder = SearchIteratorParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withOutFields(Lists.newArrayList(ID_FIELD))
                .withBatchSize(batchSize)
                .withVectorFieldName(VECTOR_FIELD)
                .withVectors(vectors)
                .withParams(params)
                .withMetricType(MetricType.L2);

        if (topK != null) {
            searchIteratorParamBuilder.withTopK(topK);
        }

        R<SearchIterator> response = milvusClient.searchIterator(searchIteratorParamBuilder.build());
        CommonUtils.handleResponseStatus(response);
        return response.getData();
    }

    private String buildSearchParams() {
        return "{}";
    }

    public static void main(String[] args) {
        boolean skipDataPeriod = false;

        IteratorExample example = new IteratorExample();
        example.reCreateCollection();
        if (!skipDataPeriod) {
            example.prepareData();
        }

        example.queryIterateCollectionNoOffset();
        example.queryIterateCollectionWithOffset();
        example.queryIterateCollectionWithLimit();

        example.searchIteratorCollection();
        example.searchIteratorCollectionWithLimit();
    }
}
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

package io.milvus.v2;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.SearchIterator;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class IteratorExample {
    private static final MilvusClientV2 client;

    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_iterator_v2";
    private static final String ID_FIELD = "userID";
    private static final String AGE_FIELD = "userAge";
    private static final String VECTOR_FIELD = "userFace";
    private static final Integer VECTOR_DIM = 128;

    private static void buildCollection() {
        // Create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(AGE_FIELD)
                .dataType(DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);

        // Insert rows
        long count = 10000;
        List<JsonObject> rowsData = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0L; i < count; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.addProperty(AGE_FIELD, ran.nextInt(99));
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            rowsData.add(row);
        }
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rowsData)
                .build());

        // Check row count
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        System.out.printf("Inserted row count: %d\n", queryResults.get(0).getEntity().get("count(*)"));
    }

    // Query iterator
    private static void queryIterator(String expr, int batchSize, int offset, int limit) {
        System.out.println("\n========== queryIterator() ==========");
        System.out.println(String.format("expr='%s', batchSize=%d, offset=%d, limit=%d", expr, batchSize, offset, limit));
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .expr(expr)
                .outputFields(Lists.newArrayList(ID_FIELD, AGE_FIELD))
                .batchSize(batchSize)
                .offset(offset)
                .limit(limit)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        System.out.println("QueryIterator results:");
        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.println("query iteration finished, close");
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
                counter++;
            }
        }
        System.out.printf("%d query results returned%n", counter);
    }

    private static void queryIteratorWithTemplate(int batchSize) {
        System.out.println("\n========== queryIterator() ==========");
        List<Long> ids = new ArrayList<>();
        for (long i = 500L; i < 600L; i++) {
            ids.add(i);
        }
        Map<String, Object> template = new HashMap<>();
        template.put("my_ids", ids);

        String filter = ID_FIELD + " in {my_ids}";
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .expr(filter)
                .outputFields(Lists.newArrayList(ID_FIELD, AGE_FIELD))
                .batchSize(batchSize)
                .filterTemplateValues(template)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        System.out.println("QueryIterator with filter template results:");
        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                System.out.println("query iteration finished, close");
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
                counter++;
            }
        }
        System.out.printf("%d query results returned%n", counter);
    }


    // Search iterator V1
    private static void searchIteratorV1(String expr, String params, int batchSize, int topK) {
        System.out.println("\n========== searchIteratorV1() ==========");
        System.out.println(String.format("expr='%s', params='%s', batchSize=%d, topK=%d", expr, params, batchSize, topK));
        SearchIterator searchIterator = client.searchIterator(SearchIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Lists.newArrayList(AGE_FIELD))
                .batchSize(batchSize)
                .vectorFieldName(VECTOR_FIELD)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .expr(expr)
                .params(StringUtils.isEmpty(params) ? "{}" : params)
                .limit(topK)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        System.out.println("SearchIteratorV1 results:");
        int counter = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("Search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
                counter++;
            }
        }
        System.out.printf("%d search results returned\n%n", counter);
    }

    // Search iterator V2
    // In SDK v2.5.6, we provide a new search iterator implementation. SearchIteratorV2 is recommended.
    // SearchIteratorV2 is faster than V1 by 20~30 percent, and the recall is a little better than V1.
    private static void searchIteratorV2(String filter, Map<String, Object> params, int batchSize, int topK,
                                         Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc) {
        System.out.println("\n========== searchIteratorV2() ==========");
        System.out.println(String.format("expr='%s', params='%s', batchSize=%d, topK=%d",
                filter, params == null ? "" : params.toString(), batchSize, topK));
        SearchIteratorV2 searchIterator = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Lists.newArrayList(AGE_FIELD))
                .batchSize(batchSize)
                .vectorFieldName(VECTOR_FIELD)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .filter(filter)
                .searchParams(params == null ? new HashMap<>() : params)
                .limit(topK)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .externalFilterFunc(externalFilterFunc)
                .build());

        System.out.println("SearchIteratorV2 results:");
        int counter = 0;
        while (true) {
            List<SearchResp.SearchResult> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("Search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (SearchResp.SearchResult record : res) {
                System.out.println(record);
                counter++;
            }
        }
        System.out.printf("%d search results returned\n%n", counter);
    }

    private static void searchIteratorV2WithTemplate(int batchSize) {
        System.out.println("\n========== searchIteratorV2() ==========");
        List<Long> ids = new ArrayList<>();
        for (long i = 500L; i < 600L; i++) {
            ids.add(i);
        }
        Map<String, Object> template = new HashMap<>();
        template.put("my_ids", ids);

        String filter = ID_FIELD + " in {my_ids}";
        SearchIteratorV2 searchIterator = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Lists.newArrayList(AGE_FIELD))
                .batchSize(batchSize)
                .vectorFieldName(VECTOR_FIELD)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .filter(filter)
                .filterTemplateValues(template)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        System.out.println("SearchIteratorV2 with filter template results:");
        int counter = 0;
        while (true) {
            List<SearchResp.SearchResult> res = searchIterator.next();
            if (res.isEmpty()) {
                System.out.println("Search iteration finished, close");
                searchIterator.close();
                break;
            }

            for (SearchResp.SearchResult record : res) {
                System.out.println(record);
                counter++;
            }
        }
        System.out.printf("%d search results returned\n%n", counter);
    }

    public static void main(String[] args) {
        buildCollection();

        // set rpcTimeoutMs, just to verify it works for each call of query/search inside the iterator
        // in versions older than 2.5.16/2.6.11, iterator.next() will timeout after several calls if the rpcTimeoutMs is greater than 0
        client.withTimeout(200, TimeUnit.MILLISECONDS);

        queryIterator("userID < 3000", 1, 5, 10000);
        queryIteratorWithTemplate(80);

        searchIteratorV1("userAge > 50 &&userAge < 100", "{\"range_filter\": 15.0, \"radius\": 20.0}", 100, 500);
        searchIteratorV1("", "", 1, 3000);
        searchIteratorV2("userAge > 10 &&userAge < 20", null, 50, 120, null);

        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("radius", 15.0);
        searchIteratorV2("", extraParams, 50, 100, null);
        searchIteratorV2WithTemplate(80);

        // use external function to filter the result
        Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = (List<SearchResp.SearchResult> src) -> {
            List<SearchResp.SearchResult> newRes = new ArrayList<>();
            for (SearchResp.SearchResult res : src) {
                long id = (long) res.getId();
                if (id % 2 == 0) {
                    newRes.add(res);
                }
            }
            return newRes;
        };
        searchIteratorV2("userAge < 20", null, 50, 88, externalFilterFunc);
    }
}

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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.orm.iterator.SearchIteratorV2;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchIteratorReqV2;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class IteratorSearchExample {
    private static final MilvusClientV2 client;

    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_iterator_search_v2";
    private static final String USER_ID = "user_id";
    private static final String USER_NAME = "user_name";
    private static final String USER_AGE = "user_age";
    private static final String USER_FACE = "user_face";
    private static final int VECTOR_DIM = 128;
    private static final Gson GSON = new Gson();

    private static long getRowCount() {
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        long count = ((Number) queryResp.getQueryResults().get(0).getEntity().get("count(*)")).longValue();
        System.out.println("count(*) = " + count);
        return count;
    }

    private static void buildCollection(IndexParam.MetricType metric) {
        // collection schema, drop and create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(USER_ID)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.FALSE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(USER_NAME)
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(USER_AGE)
                .dataType(DataType.Int8)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(USER_FACE)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Note that we use FLAT here, because other indexes cannot ensure that all entities can be fetched by iterator
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(USER_FACE)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(metric)
                .build());
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexParams)
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // insert rows with unordered primary keys
        // the pk will be 50000~59999, 10000~19999, 30000~39999, 90000~99999, 0~9999
        long[] pkSeeds = new long[] {50000L, 10000L, 30000L, 90000L, 0L};
        for (long seed : pkSeeds) {
            List<JsonObject> rows = new ArrayList<>();
            for (long k = 0; k < 10000; k++) {
                long id = seed + k;
                JsonObject row = new JsonObject();
                row.addProperty(USER_ID, id);
                row.addProperty(USER_NAME, "my name is " + id);
                row.addProperty(USER_AGE, k % 100);
                row.add(USER_FACE, GSON.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
                row.addProperty("a", id);                            // dynamic field "a"
                row.addProperty("b", "b is " + id);                 // dynamic field "b"
                rows.add(row);
            }
            InsertResp insertResp = client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
            System.out.println(insertResp.getInsertCnt() + " rows inserted.");
        }
        getRowCount();
    }

    // Note: SearchIterator doesn't allow to set offset value
    private static void iterateCollection(int batchSize, int limit, String filter, IndexParam.MetricType metric) {
        System.out.println("=====================================================");
        System.out.println("Iterate batch: " + batchSize + " limit: " + limit + " filter: " + filter);
        SearchIteratorV2 searchIterator = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Arrays.asList(USER_NAME, USER_AGE, "b"))
                .batchSize(batchSize)
                .vectorFieldName(USER_FACE)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .filter(filter)
                .limit(limit)
                .metricType(metric)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        Set<Long> ids = new HashSet<>();
        int pages = 0;
        long totalCount = 0;
        while (true) {
            List<SearchResp.SearchResult> results = searchIterator.next();
            if (results.isEmpty()) {
                System.out.println("search iteration finished");
                break;
            }
            pages++;
            totalCount += results.size();
            System.out.println("No." + pages + " page " + results.size() + " rows fetched");
            System.out.println("\tthe first row: " + results.get(0));
            System.out.println("\tthe last row: " + results.get(results.size() - 1));
            for (SearchResp.SearchResult result : results) {
                ids.add(((Number) result.getId()).longValue());
            }
        }
        searchIterator.close();

        // verify the number of returned ids is expected
        // only check when filter is empty because filtering is unpredictable
        if (StringUtils.isEmpty(filter)) {
            long rowCount = getRowCount();
            long expectedCount = Math.min(limit, rowCount);
            if (limit < 0) {
                expectedCount = rowCount;
            }
            if (ids.size() != expectedCount) {
                System.out.println("Returned row count is unexpected: " + ids.size() + " returned vs "
                        + "limit " + expectedCount);
                System.out.println("Possible reason: some vectors' distance are equal, search engine could not distinct");
            }
        }
        System.out.println("Total fetched rows: " + totalCount);
        System.out.println("=====================================================");
    }

    // Demonstrates the client-side page filter (pymilvus external_filter_func).
    // The callback receives each page of hits fetched by the iterator and prunes rows.
    // Here we keep only rows whose user_age is even.
    private static void iterateWithExternalFilter(int batchSize, int limit, String filter, IndexParam.MetricType metric) {
        System.out.println("=====================================================");
        System.out.println("Iterate with external filter, batch: " + batchSize + " limit: " + limit
                + " filter: " + filter);
        Function<List<SearchResp.SearchResult>, List<SearchResp.SearchResult>> externalFilterFunc = (src) -> {
            List<SearchResp.SearchResult> newRes = new ArrayList<>();
            for (SearchResp.SearchResult res : src) {
                if (((Number) res.getEntity().get(USER_AGE)).intValue() % 2 == 0) {
                    newRes.add(res);
                }
            }
            return newRes;
        };
        SearchIteratorV2 searchIterator = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Arrays.asList(USER_NAME, USER_AGE, "b"))
                .batchSize(batchSize)
                .vectorFieldName(USER_FACE)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .filter(filter)
                .limit(limit)
                .metricType(metric)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .externalFilterFunc(externalFilterFunc)
                .build());

        Set<Long> ids = new HashSet<>();
        int pages = 0;
        long totalCount = 0;
        while (true) {
            List<SearchResp.SearchResult> results = searchIterator.next();
            if (results.isEmpty()) {
                System.out.println("search iteration with external filter finished");
                break;
            }
            pages++;
            totalCount += results.size();
            System.out.println("No." + pages + " page " + results.size() + " rows fetched");
            for (SearchResp.SearchResult result : results) {
                if (((Number) result.getEntity().get(USER_AGE)).intValue() % 2 != 0) {
                    System.out.println("ERROR: a row with odd user_age slipped through the external filter");
                }
                ids.add(((Number) result.getId()).longValue());
            }
        }
        searchIterator.close();
        System.out.println("Total fetched rows (external filter): " + totalCount);
        System.out.println("=====================================================");
    }

    // search iterator with extra search params (e.g. radius / range_filter for range search)
    private static void searchIteratorV2WithParams(int batchSize, int topK, Map<String, Object> params,
                                                  IndexParam.MetricType metric) {
        System.out.println("=====================================================");
        System.out.println("Iterate with search params: " + (params == null ? "" : params.toString())
                + ", batch: " + batchSize + " limit: " + topK + " filter: ");
        SearchIteratorV2 searchIterator = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Arrays.asList(USER_NAME, USER_AGE, "b"))
                .batchSize(batchSize)
                .vectorFieldName(USER_FACE)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .searchParams(params)
                .limit(topK)
                .metricType(metric)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        int pages = 0;
        long totalCount = 0;
        while (true) {
            List<SearchResp.SearchResult> results = searchIterator.next();
            if (results.isEmpty()) {
                System.out.println("search iteration finished");
                break;
            }
            pages++;
            totalCount += results.size();
            System.out.println("No." + pages + " page " + results.size() + " rows fetched");
        }
        searchIterator.close();
        System.out.println("Total fetched rows: " + totalCount);
        System.out.println("=====================================================");
    }

    // search iterator with a filter template: the filter references a template key, the actual
    // id list is supplied via filterTemplateValues
    private static void searchIteratorV2WithTemplate(int batchSize, IndexParam.MetricType metric) {
        System.out.println("=====================================================");
        List<Long> ids = new ArrayList<>();
        for (long i = 500L; i < 600L; i++) {
            ids.add(i);
        }
        Map<String, Object> template = new HashMap<>();
        template.put("my_ids", ids);

        String filter = USER_ID + " in {my_ids}";
        System.out.println("Iterate batch: " + batchSize + " limit: -1 filter: " + filter + " (filter template)");
        SearchIteratorV2 searchIterator = client.searchIteratorV2(SearchIteratorReqV2.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Arrays.asList(USER_NAME, USER_AGE, "b"))
                .batchSize(batchSize)
                .vectorFieldName(USER_FACE)
                .vectors(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .filter(filter)
                .filterTemplateValues(template)
                .limit(-1)
                .metricType(metric)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        int pages = 0;
        long totalCount = 0;
        while (true) {
            List<SearchResp.SearchResult> results = searchIterator.next();
            if (results.isEmpty()) {
                System.out.println("search iteration finished");
                break;
            }
            pages++;
            totalCount += results.size();
            System.out.println("No." + pages + " page " + results.size() + " rows fetched");
        }
        searchIterator.close();
        System.out.println("Total fetched rows: " + totalCount);
        System.out.println("=====================================================");
    }

    public static void main(String[] args) {
        try {
            for (IndexParam.MetricType metric : Arrays.asList(IndexParam.MetricType.L2, IndexParam.MetricType.COSINE)) {
                buildCollection(metric);

                iterateCollection(3000, 100000, "", metric);
                iterateCollection(25, 80, "", metric);
                iterateCollection(5000, -1, "", metric);
                iterateCollection(100, -1, USER_AGE + " == 8", metric);
                iterateCollection(1000, 2500, USER_AGE + " > 30", metric);
                iterateCollection(1000, 100000, USER_AGE + " in [30, 40, 50]", metric);

                Map<String, Object> extraParams = new HashMap<>();
                // range-search bound: cosine distances are small, L2 distances are not
                extraParams.put("radius", metric == IndexParam.MetricType.COSINE ? 0.8 : 15.0);
                searchIteratorV2WithParams(50, 100, extraParams, metric);

                searchIteratorV2WithTemplate(80, metric);

                iterateWithExternalFilter(3000, 100000, "", metric);
            }
        } finally {
            client.close();
        }
    }
}

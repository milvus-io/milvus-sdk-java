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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.orm.iterator.QueryIteratorCursor;
import io.milvus.param.Constant;
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
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IteratorQueryExample {
    private static final MilvusClientV2 client;

    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_iterator_query_v2";
    private static final String USER_ID = "user_id";
    private static final String USER_NAME = "user_name";
    private static final String USER_AGE = "user_age";
    private static final String USER_FACE = "user_face";
    private static final String USER_TAGS = "user_tags";
    private static final String TAG_FIELD = "tag";
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

    private static void buildCollection() {
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
        // user_tags is a struct-array field: each element is a struct { tag } and element_filter
        // matches inside the array (element_filter() only supports struct-array fields)
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(USER_TAGS)
                .description("user tags")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(10)
                .addStructField(AddFieldReq.builder()
                        .fieldName(TAG_FIELD)
                        .dataType(DataType.VarChar)
                        .maxLength(100)
                        .build())
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(USER_FACE)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(USER_FACE)
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.L2)
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
                // some rows carry a "news" tag element (some even twice) so the element_filter
                // demo below has matching elements to iterate over
                JsonArray tags = new JsonArray();
                JsonObject sports = new JsonObject();
                sports.addProperty(TAG_FIELD, "sports");
                tags.add(sports);
                if (k % 3 == 0) {
                    JsonObject news = new JsonObject();
                    news.addProperty(TAG_FIELD, "news");
                    tags.add(news);
                }
                if (k % 7 == 0) {
                    JsonObject news = new JsonObject();
                    news.addProperty(TAG_FIELD, "news");
                    tags.add(news);
                }
                row.add(USER_TAGS, tags);
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

    private static void iterateQuery(int batchSize, int offset, int limit, String expr) {
        System.out.println("=====================================================");
        System.out.println("Iterate batch: " + batchSize + " offset: " + offset
                + " limit: " + limit + " expr: " + expr);
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .expr(expr)
                .outputFields(Arrays.asList(USER_NAME, USER_AGE, "a"))
                .batchSize(batchSize)
                .offset(offset)
                .limit(limit)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        Set<Long> ids = new HashSet<>();
        int pages = 0;
        long totalCount = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> records = queryIterator.next();
            if (records.isEmpty()) {
                System.out.println("query iteration finished");
                break;
            }
            pages++;
            totalCount += records.size();
            System.out.println("No." + pages + " page " + records.size() + " rows fetched");
            System.out.println("\tthe first row: " + records.get(0));
            System.out.println("\tthe last row: " + records.get(records.size() - 1));
            for (QueryResultsWrapper.RowRecord record : records) {
                ids.add(((Number) record.get(USER_ID)).longValue());
            }
        }
        queryIterator.close();

        // verify the number of returned ids is expected
        // only check when expr is empty because filtering is unpredictable
        if (StringUtils.isEmpty(expr)) {
            long rowCount = getRowCount();
            long expectedCount = Math.min(limit, rowCount - offset);
            if (limit < 0) {
                expectedCount = rowCount - offset;
            }
            if (ids.size() != expectedCount) {
                throw new IllegalStateException("Returned row count is unexpected: " + ids.size()
                        + " returned vs expected " + expectedCount);
            }
        }
        System.out.println("Total fetched rows: " + totalCount);
        System.out.println("=====================================================");
    }

    // query iterator with a filter template: the expr references a template key, the actual
    // id list is supplied via filterTemplateValues
    private static void queryIteratorWithTemplate(int batchSize) {
        System.out.println("=====================================================");
        List<Long> ids = new ArrayList<>();
        for (long i = 500L; i < 600L; i++) {
            ids.add(i);
        }
        Map<String, Object> template = new HashMap<>();
        template.put("my_ids", ids);

        String expr = USER_ID + " in {my_ids}";
        System.out.println("Iterate batch: " + batchSize + " limit: -1 expr: " + expr + " (filter template)");
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .expr(expr)
                .outputFields(Arrays.asList(USER_NAME, USER_AGE))
                .batchSize(batchSize)
                .filterTemplateValues(template)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        int pages = 0;
        long totalCount = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> records = queryIterator.next();
            if (records.isEmpty()) {
                System.out.println("query iteration finished");
                break;
            }
            pages++;
            totalCount += records.size();
            System.out.println("No." + pages + " page " + records.size() + " rows fetched");
            for (QueryResultsWrapper.RowRecord record : records) {
                System.out.println(record);
            }
        }
        queryIterator.close();
        System.out.println("Total fetched rows: " + totalCount);
        System.out.println("=====================================================");
    }

    // capture a cursor after the first page and resume a brand-new iterator from it
    private static void queryIteratorWithCursor(int batchSize, int limit) {
        System.out.println("=====================================================");
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList(USER_NAME))
                .batchSize(batchSize)
                .limit(limit)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        List<QueryResultsWrapper.RowRecord> firstPage = queryIterator.next();
        System.out.println("First page fetched " + firstPage.size() + " rows.");

        QueryIteratorCursor cursor = queryIterator.getCursor();
        queryIterator.close();

        // the limit is per-iterator, so subtract the rows already delivered to keep the
        // combined total of both iterators equal to the configured limit
        QueryIterator resumed = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList(USER_NAME))
                .batchSize(batchSize)
                .limit(limit - firstPage.size())
                .cursor(cursor)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        List<QueryResultsWrapper.RowRecord> resumedPage = resumed.next();
        System.out.println("Resumed iterator fetched " + resumedPage.size() + " rows.");
        resumed.close();
        System.out.println("=====================================================");
    }

    // iterate the matched elements of an element_filter expression: the iterator expands each
    // entity into one row per matched element and appends an "offset" column with the element
    // position inside the entity's array field, so every returned row is a real matched element.
    private static void iterateElementFilter() {
        System.out.println("=====================================================");
        String filter = String.format("element_filter(%s, $[%s] == \"news\")", USER_TAGS, TAG_FIELD);
        System.out.println("Iterate " + filter);
        QueryIterator queryIterator = client.queryIterator(QueryIteratorReq.builder()
                .collectionName(COLLECTION_NAME)
                .expr(filter)
                .outputFields(Arrays.asList(USER_ID, USER_TAGS))
                .batchSize(100)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        int pages = 0;
        long totalElements = 0;
        while (true) {
            List<QueryResultsWrapper.RowRecord> records = queryIterator.next();
            if (records.isEmpty()) {
                System.out.println("element-filter iteration finished");
                break;
            }
            pages++;
            totalElements += records.size();
            System.out.println("No." + pages + " page " + records.size() + " matched elements: ");
            for (QueryResultsWrapper.RowRecord record : records) {
                System.out.println("[id=" + record.get(USER_ID) + " tags=" + record.get(USER_TAGS)
                        + " offset=" + record.get(Constant.OFFSET) + "] ");
            }
            System.out.println();
        }
        queryIterator.close();
        System.out.println("Total fetched matched elements: " + totalElements);
        System.out.println("=====================================================");
    }

    public static void main(String[] args) {
        try {
            buildCollection();

            iterateQuery(3000, 25000, 100000, "");
            iterateQuery(25, 100, 80, "");
            iterateQuery(5000, 0, -1, "");
            iterateQuery(100, 0, -1, USER_AGE + " == 8");
            iterateQuery(1000, 15000, 2500, USER_AGE + " > 30");
            iterateQuery(1000, 0, 100000, USER_AGE + " in [30, 40, 50]");

            queryIteratorWithTemplate(80);

            // batch 20 so the first page (20 rows) does not consume the whole limit of 50;
            // the resumed iterator then continues with the remaining 50 - 20 = 30 rows
            queryIteratorWithCursor(20, 50);
            iterateElementFilter();
        } finally {
            client.close();
        }
    }
}

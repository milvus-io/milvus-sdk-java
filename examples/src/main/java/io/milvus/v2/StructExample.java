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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.EmbeddingList;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class StructExample {
    private static final MilvusClientV2 client;

    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_struct_v2";
    private static final String ID_FIELD = "id";
    private static final String NAME_FIELD = "film_name";
    private static final String STRUCT_FIELD = "clips";
    private static final String FRAME_FIELD = "frame_number";
    private static final String CLIP_VECTOR_FIELD = "clip_embedding";
    private static final String DESC_FIELD = "clip_desc";
    private static final String DESC_VECTOR_FIELD = "description_embedding";
    private static final String EXTRA_STRUCT_FIELD = "metadata";
    private static final String RATING_FIELD = "rating";
    private static final String TAG_FIELD = "tag";
    private static final Integer VECTOR_DIM = 128;
    private static final Random RANDOM = new Random();
    private static final long EXTRA_ROW_ID = 5000L;

    private static void createCollection() {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(NAME_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(1024)
                .build());

        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(STRUCT_FIELD)
                .description("clips of a film")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName(FRAME_FIELD)
                        .description("from which frame this clip begin")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(CLIP_VECTOR_FIELD)
                        .description("embedding of a clip")
                        .dataType(DataType.FloatVector)
                        .dimension(VECTOR_DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(DESC_FIELD)
                        .description("description of a clip")
                        .dataType(DataType.VarChar)
                        .maxLength(1024)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(DESC_VECTOR_FIELD)
                        .description("embedding of description")
                        .dataType(DataType.FloatVector)
                        .dimension(VECTOR_DIM)
                        .build())
                .build());

        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("simplify_clips")
                .description("simplify clips")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName(CLIP_VECTOR_FIELD)
                        .description("clip has been simplified")
                        .dataType(DataType.FloatVector)
                        .dimension(32)
                        .build())
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, CLIP_VECTOR_FIELD))
                .indexName("index_1")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.MAX_SIM_L2)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, DESC_VECTOR_FIELD))
                .indexName("index_2")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.MAX_SIM_IP)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("simplify_clips[%s]", CLIP_VECTOR_FIELD))
                .indexName("index_3")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.MAX_SIM_COSINE)
                .build());
        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(indexParams)
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection created: " + COLLECTION_NAME);

        describeCollection();
    }

    private static void describeCollection() {
        DescribeCollectionResp resp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println(resp.getCollectionSchema());
    }

    private static JsonObject buildBaseRow(long id) {
        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, id);
        row.addProperty(NAME_FIELD, "film_" + id);

        JsonArray structArr = new JsonArray();
        for (int k = 0; k < 5; k++) {
            JsonObject struct = new JsonObject();
            struct.addProperty(FRAME_FIELD, RANDOM.nextInt(10000));
            struct.add(CLIP_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            struct.addProperty(DESC_FIELD, "clip_description_" + id);
            struct.add(DESC_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            structArr.add(struct);
        }
        row.add(STRUCT_FIELD, structArr);

        JsonArray simplifyClips = new JsonArray();
        for (int k = 0; k < 2; k++) {
            JsonObject struct = new JsonObject();
            struct.add(CLIP_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(32)));
            simplifyClips.add(struct);
        }
        row.add("simplify_clips", simplifyClips);
        return row;
    }

    private static void insertData(int rowCount) {
        final int batchSize = 300;
        int insertedCount = 0;
        while (insertedCount < rowCount) {
            int nextBatch = Math.min(batchSize, rowCount - insertedCount);
            List<JsonObject> rows = new ArrayList<>();
            for (int i = 0; i < nextBatch; i++) {
                rows.add(buildBaseRow(insertedCount + i));
            }

            InsertResp insertResp = client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
            insertedCount += (int) insertResp.getInsertCnt();
            System.out.println("Inserted row count: " + insertResp.getInsertCnt());
        }

        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static List<QueryResp.QueryResult> query(String filter) {
        System.out.println("===================================================");
        System.out.println("Query with filter expression: " + filter);
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(filter)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Arrays.asList(STRUCT_FIELD, "simplify_clips"))
                .build());
        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        for (QueryResp.QueryResult result : queryResults) {
            System.out.println(result.getEntity());
        }
        return queryResults;
    }

    private static void search(String annsField, List<BaseVector> searchData) {
        System.out.println("===================================================");
        System.out.printf("Search on field '%s' in struct '%s' with nq=%d\n",
                annsField, STRUCT_FIELD, searchData.size());

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(String.format("%s[%s]", STRUCT_FIELD, annsField))
                .data(searchData)
                .limit(5)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Arrays.asList(NAME_FIELD,
                        String.format("%s[%s]", STRUCT_FIELD, FRAME_FIELD),
                        String.format("%s[%s]", STRUCT_FIELD, DESC_FIELD),
                        String.format("simplify_clips[%s]", CLIP_VECTOR_FIELD)))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (int i = 0; i < searchResults.size(); i++) {
            System.out.println("Results of No." + i + " embedding list");
            for (SearchResp.SearchResult result : searchResults.get(i)) {
                System.out.println(result);
            }
        }
    }

    private static JsonArray buildMetadataStructArray() {
        JsonArray metadata = new JsonArray();

        JsonObject first = new JsonObject();
        first.addProperty(RATING_FIELD, 5);
        first.addProperty(TAG_FIELD, "favorite");
        metadata.add(first);

        JsonObject second = new JsonObject();
        second.addProperty(RATING_FIELD, 4);
        second.addProperty(TAG_FIELD, "classic");
        metadata.add(second);
        return metadata;
    }

    private static void addCollectionStructField() {
        System.out.println("===================================================");
        System.out.println("Add a new struct field to the existing collection");
        client.addCollectionStructField(AddCollectionStructFieldReq.builder()
                .collectionName(COLLECTION_NAME)
                .fieldName(EXTRA_STRUCT_FIELD)
                .description("additional metadata for films")
                .maxCapacity(8)
                .addStructField(AddFieldReq.builder()
                        .fieldName(RATING_FIELD)
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(TAG_FIELD)
                        .dataType(DataType.VarChar)
                        .maxLength(128)
                        .build())
                .build());
        System.out.println("Added struct field: " + EXTRA_STRUCT_FIELD);

        describeCollection();

        QueryResp existingRow = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " == 5")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(ID_FIELD, EXTRA_STRUCT_FIELD))
                .build());
        System.out.println("Existing row after addCollectionStructField() - new field is null");
        for (QueryResp.QueryResult result : existingRow.getQueryResults()) {
            System.out.println(result.getEntity());
        }

        JsonObject newRow = buildBaseRow(EXTRA_ROW_ID);
        newRow.add(EXTRA_STRUCT_FIELD, buildMetadataStructArray());
        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(newRow))
                .build());

        QueryResp newRowResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " == " + EXTRA_ROW_ID)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(ID_FIELD, NAME_FIELD, EXTRA_STRUCT_FIELD))
                .build());
        System.out.println("New row with the added struct field");
        for (QueryResp.QueryResult result : newRowResp.getQueryResults()) {
            System.out.println(result.getEntity());
        }

        QueryResp projectedResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " == " + EXTRA_ROW_ID)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(ID_FIELD, NAME_FIELD,
                        String.format("%s[%s]", EXTRA_STRUCT_FIELD, RATING_FIELD),
                        String.format("%s[%s]", EXTRA_STRUCT_FIELD, TAG_FIELD)))
                .build());
        System.out.println("Projected subfields from the added struct field");
        for (QueryResp.QueryResult result : projectedResp.getQueryResults()) {
            System.out.println(result.getEntity());
        }
    }

    public static void main(String[] args) {
        try {
            createCollection();
            insertData(2000);

            List<QueryResp.QueryResult> results = query(ID_FIELD + " in [5, 8]");
            for (QueryResp.QueryResult result : results) {
                Map<String, Object> fetchedEntity = result.getEntity();
                List<Map<String, Object>> structs = (List<Map<String, Object>>) fetchedEntity.get(STRUCT_FIELD);
                EmbeddingList embList = new EmbeddingList();
                for (Map<String, Object> struct : structs) {
                    List<Float> vector = (List<Float>) struct.get(CLIP_VECTOR_FIELD);
                    embList.add(new FloatVec(vector));
                }
                search(CLIP_VECTOR_FIELD, Collections.singletonList(embList));
            }

            addCollectionStructField();
        } finally {
            client.close();
        }
    }
}

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
import io.milvus.v2.service.collection.request.AddCollectionStructFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BFloat16Vec;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.EmbeddingList;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.Int8Vec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

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
    private static final String DESC_FIELD = "clip_desc";
    private static final String FLOAT_VECTOR_FIELD = "clip_float_embedding";
    private static final String BINARY_VECTOR_FIELD = "clip_binary_embedding";
    private static final String FLOAT16_VECTOR_FIELD = "clip_float16_embedding";
    private static final String BFLOAT16_VECTOR_FIELD = "clip_bfloat16_embedding";
    private static final String INT8_VECTOR_FIELD = "clip_int8_embedding";
    private static final String EXTRA_STRUCT_FIELD = "metadata";
    private static final String RATING_FIELD = "rating";
    private static final String TAG_FIELD = "tag";
    private static final int FLOAT_VECTOR_DIM = 128;
    private static final int BINARY_VECTOR_DIM = 32;
    private static final int FLOAT16_VECTOR_DIM = 32;
    private static final int INT8_VECTOR_DIM = 32;
    private static final int IVF_NLIST = 64;
    private static final IndexParam.MetricType FLOAT_VECTOR_METRIC = IndexParam.MetricType.MAX_SIM_IP;
    private static final IndexParam.MetricType BINARY_VECTOR_METRIC = IndexParam.MetricType.MAX_SIM_HAMMING;
    private static final IndexParam.MetricType FLOAT16_VECTOR_METRIC = IndexParam.MetricType.MAX_SIM_COSINE;
    private static final IndexParam.MetricType INT8_VECTOR_METRIC = IndexParam.MetricType.MAX_SIM_L2;
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
                        .fieldName(DESC_FIELD)
                        .description("description of a clip")
                        .dataType(DataType.VarChar)
                        .maxLength(1024)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(FLOAT_VECTOR_FIELD)
                        .description("float embedding of a clip")
                        .dataType(DataType.FloatVector)
                        .dimension(FLOAT_VECTOR_DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(BINARY_VECTOR_FIELD)
                        .description("binary embedding of a clip")
                        .dataType(DataType.BinaryVector)
                        .dimension(BINARY_VECTOR_DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(FLOAT16_VECTOR_FIELD)
                        .description("float16 embedding of a clip")
                        .dataType(DataType.Float16Vector)
                        .dimension(FLOAT16_VECTOR_DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(BFLOAT16_VECTOR_FIELD)
                        .description("bfloat16 embedding of a clip")
                        .dataType(DataType.BFloat16Vector)
                        .dimension(FLOAT16_VECTOR_DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(INT8_VECTOR_FIELD)
                        .description("int8 embedding of a clip")
                        .dataType(DataType.Int8Vector)
                        .dimension(INT8_VECTOR_DIM)
                        .build())
                .build());

        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("simplify_clips")
                .description("simplify clips")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName(FLOAT_VECTOR_FIELD)
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
        Map<String, Object> ivfExtraParams = new HashMap<>();
        ivfExtraParams.put("nlist", IVF_NLIST);
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, FLOAT_VECTOR_FIELD))
                .indexName("index_float")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(FLOAT_VECTOR_METRIC)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, BINARY_VECTOR_FIELD))
                .indexName("index_binary")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(BINARY_VECTOR_METRIC)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, FLOAT16_VECTOR_FIELD))
                .indexName("index_float16")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(FLOAT16_VECTOR_METRIC)
                .extraParams(ivfExtraParams)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, BFLOAT16_VECTOR_FIELD))
                .indexName("index_bfloat16")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(FLOAT16_VECTOR_METRIC)
                .extraParams(ivfExtraParams)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, INT8_VECTOR_FIELD))
                .indexName("index_int8")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(INT8_VECTOR_METRIC)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("simplify_clips[%s]", FLOAT_VECTOR_FIELD))
                .indexName("index_simplify")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(FLOAT_VECTOR_METRIC)
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
            struct.addProperty(DESC_FIELD, "clip_description_" + id);
            struct.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
            struct.add(BINARY_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM).array()));
            struct.add(FLOAT16_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloat16Vector(FLOAT16_VECTOR_DIM, false).array()));
            struct.add(BFLOAT16_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloat16Vector(FLOAT16_VECTOR_DIM, true).array()));
            struct.add(INT8_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateInt8Vector(INT8_VECTOR_DIM).array()));
            structArr.add(struct);
        }
        row.add(STRUCT_FIELD, structArr);

        JsonArray simplifyClips = new JsonArray();
        for (int k = 0; k < 2; k++) {
            JsonObject struct = new JsonObject();
            struct.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(32)));
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
                .limit(3)
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
                        String.format("simplify_clips[%s]", FLOAT_VECTOR_FIELD)))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (int i = 0; i < searchResults.size(); i++) {
            System.out.println("Results of No." + i + " embedding list");
            for (SearchResp.SearchResult result : searchResults.get(i)) {
                System.out.println(result);
            }
        }
    }

    private static void searchStructField(String annsField,
                                          String filter,
                                          Function<Map<String, Object>, BaseVector> converter,
                                          String title) {
        List<QueryResp.QueryResult> queryResults = query(filter);
        List<BaseVector> searchData = new ArrayList<>();
        for (QueryResp.QueryResult result : queryResults) {
            List<Map<String, Object>> structs = (List<Map<String, Object>>) result.getEntity().get(STRUCT_FIELD);
            EmbeddingList embList = new EmbeddingList();
            for (Map<String, Object> struct : structs) {
                embList.add(converter.apply(struct));
            }
            searchData.add(embList);
        }

        search(annsField, searchData);
    }

    private static void searchFloatVectorField(String filter) {
        searchStructField(
                FLOAT_VECTOR_FIELD,
                filter,
                struct -> new FloatVec((List<Float>) struct.get(FLOAT_VECTOR_FIELD)),
                "Search on float field '" + FLOAT_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
    }

    private static void searchBinaryVectorField(String filter) {
        searchStructField(
                BINARY_VECTOR_FIELD,
                filter,
                struct -> new BinaryVec((ByteBuffer) struct.get(BINARY_VECTOR_FIELD)),
                "Search on binary field '" + BINARY_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
    }

    private static void searchFloat16VectorField(String filter) {
        searchStructField(
                FLOAT16_VECTOR_FIELD,
                filter,
                struct -> new Float16Vec((ByteBuffer) struct.get(FLOAT16_VECTOR_FIELD)),
                "Search on float16 field '" + FLOAT16_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
    }

    private static void searchBFloat16VectorField(String filter) {
        searchStructField(
                BFLOAT16_VECTOR_FIELD,
                filter,
                struct -> new BFloat16Vec((ByteBuffer) struct.get(BFLOAT16_VECTOR_FIELD)),
                "Search on bfloat16 field '" + BFLOAT16_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
    }

    private static void searchInt8VectorField(String filter) {
        searchStructField(
                INT8_VECTOR_FIELD,
                filter,
                struct -> new Int8Vec((ByteBuffer) struct.get(INT8_VECTOR_FIELD)),
                "Search on int8 field '" + INT8_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
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

            searchFloatVectorField(ID_FIELD + " in [5, 8]");

            String filter1 = String.format("MATCH_LEAST(%s, $[%s] < 2000, threshold=3)", STRUCT_FIELD, FRAME_FIELD);
            searchBinaryVectorField(filter1);

            searchFloat16VectorField(ID_FIELD + " < 10");

            String filter2 = String.format("MATCH_ANY(%s, $[%s] == \"clip_description_8\")", STRUCT_FIELD, DESC_FIELD);
            searchBFloat16VectorField(filter2);

            searchInt8VectorField(ID_FIELD + " == 999");

            addCollectionStructField();
        } finally {
            client.close();
        }
    }
}

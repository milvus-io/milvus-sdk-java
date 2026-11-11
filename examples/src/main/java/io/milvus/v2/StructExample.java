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
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddCollectionFieldReq;
import io.milvus.v2.service.collection.request.AddCollectionStructFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionFieldReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.EmbeddingList;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.Int8Vec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    private static final String SIMPLIFY_STRUCT_FIELD = "simplify_clips";
    private static final String FRAME_FIELD = "frame_number";
    private static final String DESC_FIELD = "clip_desc";
    private static final String FLOAT_VECTOR_FIELD = "clip_float_embedding";
    private static final String BINARY_VECTOR_FIELD = "clip_binary_embedding";
    private static final String INT8_VECTOR_FIELD = "clip_int8_embedding";
    private static final String EXTRA_STRUCT_FIELD = "metadata";
    private static final String RATING_FIELD = "rating";
    private static final String TAG_FIELD = "tag";
    private static final String SCORE_FIELD = "score";
    private static final String LANG_FIELD = "lang";
    private static final String TAGS_FIELD = "tags";
    private static final String NOTE_FIELD = "note";
    private static final String MARKS_FIELD = "marks";
    private static final int FLOAT_VECTOR_DIM = 128;
    private static final int BINARY_VECTOR_DIM = 32;
    private static final int INT8_VECTOR_DIM = 32;
    private static final IndexParam.MetricType FLOAT_VECTOR_METRIC = IndexParam.MetricType.MAX_SIM_IP;
    private static final IndexParam.MetricType BINARY_VECTOR_METRIC = IndexParam.MetricType.MAX_SIM_HAMMING;
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
                        .fieldName(INT8_VECTOR_FIELD)
                        .description("int8 embedding of a clip")
                        .dataType(DataType.Int8Vector)
                        .dimension(INT8_VECTOR_DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(SCORE_FIELD)
                        .description("score of a clip, element-level null allowed")
                        .dataType(DataType.Int64)
                        .isNullable(true)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(LANG_FIELD)
                        .description("language of a clip, element-level null allowed")
                        .dataType(DataType.VarChar)
                        .maxLength(16)
                        .isNullable(true)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(TAGS_FIELD)
                        .description("tags of a clip, a nested array with null inner values allowed")
                        .dataType(DataType.Array)
                        .elementType(DataType.VarChar)
                        .maxCapacity(8)
                        .maxLength(16)
                        .isNullable(true)
                        .elementNullable(true)
                        .build())
                .build());

        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(SIMPLIFY_STRUCT_FIELD)
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

        // make the clips struct field nullable so null rows can be inserted and read back
        collectionSchema.getStructField(STRUCT_FIELD).setNullable(true);

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
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
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, INT8_VECTOR_FIELD))
                .indexName("index_int8")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(INT8_VECTOR_METRIC)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", SIMPLIFY_STRUCT_FIELD, FLOAT_VECTOR_FIELD))
                .indexName("index_simplify")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
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

    private static JsonObject buildBaseRow(long id, boolean withClips) {
        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, id);
        row.addProperty(NAME_FIELD, "film_" + id);

        if (withClips) {
            JsonArray structArr = new JsonArray();
            for (int k = 0; k < 5; k++) {
                JsonObject struct = new JsonObject();
                struct.addProperty(FRAME_FIELD, RANDOM.nextInt(10000));
                struct.addProperty(DESC_FIELD, "clip_description_" + id);
                struct.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
                struct.add(BINARY_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM).array()));
                struct.add(INT8_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateInt8Vector(INT8_VECTOR_DIM).array()));
                struct.addProperty(SCORE_FIELD, RANDOM.nextInt(100));
                struct.addProperty(LANG_FIELD, "en");
                JsonArray tags = new JsonArray();
                tags.add("a");
                tags.add("b");
                struct.add(TAGS_FIELD, tags);
                structArr.add(struct);
            }
            row.add(STRUCT_FIELD, structArr);
        } else {
            // the clips struct field is nullable; store a null row for it
            row.add(STRUCT_FIELD, JsonNull.INSTANCE);
        }

        JsonArray simplifyClips = new JsonArray();
        for (int k = 0; k < 2; k++) {
            JsonObject struct = new JsonObject();
            struct.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(32)));
            simplifyClips.add(struct);
        }
        row.add(SIMPLIFY_STRUCT_FIELD, simplifyClips);
        return row;
    }

    // Rows demonstrating element-level null: a missing or null sub-field key is stored as a null
    // element for the element-nullable sub-fields (score/lang); nested arrays support null inner
    // values (tags: ["c", null]) and empty arrays; the whole struct row can be null (clips: null).
    private static List<JsonObject> buildElementNullRows() {
        List<JsonObject> rows = new ArrayList<>();

        JsonObject row97 = new JsonObject();
        row97.addProperty(ID_FIELD, 97L);
        row97.addProperty(NAME_FIELD, "film_97");
        JsonArray clips97 = new JsonArray();
        JsonObject c0 = new JsonObject();
        c0.addProperty(FRAME_FIELD, 0);
        c0.addProperty(DESC_FIELD, "clip_description_97");
        c0.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
        c0.add(BINARY_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM).array()));
        c0.add(INT8_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateInt8Vector(INT8_VECTOR_DIM).array()));
        c0.addProperty(SCORE_FIELD, 1L);
        c0.addProperty(LANG_FIELD, "en");
        JsonArray tags0 = new JsonArray();
        tags0.add("a");
        tags0.add("b");
        c0.add(TAGS_FIELD, tags0);
        clips97.add(c0);
        JsonObject c1 = new JsonObject();
        c1.addProperty(FRAME_FIELD, 1);
        c1.addProperty(DESC_FIELD, "clip_description_97");
        c1.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
        c1.add(BINARY_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM).array()));
        c1.add(INT8_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateInt8Vector(INT8_VECTOR_DIM).array()));
        c1.addProperty(SCORE_FIELD, 2L);
        c1.add(LANG_FIELD, JsonNull.INSTANCE); // element-level null for lang
        JsonArray tags1 = new JsonArray();
        tags1.add("c");
        tags1.add(JsonNull.INSTANCE); // null inner value in the nested array
        c1.add(TAGS_FIELD, tags1);
        clips97.add(c1);
        JsonObject c2 = new JsonObject();
        c2.addProperty(FRAME_FIELD, 2);
        c2.addProperty(DESC_FIELD, "clip_description_97");
        c2.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
        c2.add(BINARY_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM).array()));
        c2.add(INT8_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateInt8Vector(INT8_VECTOR_DIM).array()));
        c2.addProperty(SCORE_FIELD, 3L);
        // score/lang omitted => both stored as null elements; tags is an empty array
        JsonArray tags2 = new JsonArray();
        c2.add(TAGS_FIELD, tags2);
        clips97.add(c2);
        row97.add(STRUCT_FIELD, clips97);
        JsonArray simple97 = new JsonArray();
        row97.add(SIMPLIFY_STRUCT_FIELD, simple97);
        rows.add(row97);

        JsonObject row98 = new JsonObject();
        row98.addProperty(ID_FIELD, 98L);
        row98.addProperty(NAME_FIELD, "film_98");
        row98.add(STRUCT_FIELD, new JsonArray()); // empty array, a non-null row
        row98.add(SIMPLIFY_STRUCT_FIELD, new JsonArray());
        rows.add(row98);

        JsonObject row99 = new JsonObject();
        row99.addProperty(ID_FIELD, 99L);
        row99.addProperty(NAME_FIELD, "film_99");
        row99.add(STRUCT_FIELD, JsonNull.INSTANCE); // null struct row
        row99.add(SIMPLIFY_STRUCT_FIELD, new JsonArray());
        rows.add(row99);

        return rows;
    }

    // Query rows that carry element-level nulls and print how null elements / null struct rows
    // are decoded back.
    private static void verifyElementNull() {
        System.out.println("===================================================");
        System.out.println("Verify element-level null struct fields on rows 97/98/99");

        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " in [97, 98, 99]")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(ID_FIELD, STRUCT_FIELD))
                .build());
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            long id = (Long) result.getEntity().get(ID_FIELD);
            Object clips = result.getEntity().get(STRUCT_FIELD);
            System.out.println("query result id=" + id + ", clips=" + clips);
            if (id == 99L && clips != null) {
                throw new IllegalStateException("Expected null clips for id=99, got: " + clips);
            }
        }
        System.out.println("Element-level null verification passed");
    }

    private static void insertData(int rowCount) {
        final int batchSize = 300;
        int insertedCount = 0;
        long nextId = 0;
        while (insertedCount < rowCount) {
            int nextBatch = Math.min(batchSize, rowCount - insertedCount);
            List<JsonObject> rows = new ArrayList<>();
            int collected = 0;
            while (collected < nextBatch) {
                long id = nextId++;
                // rows 97/98/99 carry element-level nulls and are inserted separately,
                // so skip them here to avoid duplicate primary keys
                if (id == 97L || id == 98L || id == 99L) {
                    continue;
                }
                // insert null clips struct for every 100th row
                rows.add(buildBaseRow(id, id % 100 != 0));
                collected++;
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
                        String.format("%s[%s]", SIMPLIFY_STRUCT_FIELD, FLOAT_VECTOR_FIELD)))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (int i = 0; i < searchResults.size(); i++) {
            System.out.println("Results of No." + i + " embedding list");
            for (SearchResp.SearchResult result : searchResults.get(i)) {
                System.out.println(result);
            }
        }
    }

    private static void embeddingListSearch(String annsField,
                                            String filter,
                                            Function<Map<String, Object>, BaseVector> converter,
                                            String title) {
        List<QueryResp.QueryResult> queryResults = query(filter);
        List<BaseVector> searchData = new ArrayList<>();
        for (QueryResp.QueryResult result : queryResults) {
            List<Map<String, Object>> structs = (List<Map<String, Object>>) result.getEntity().get(STRUCT_FIELD);
            if (structs == null) {
                // this row holds a null (nullable) struct field, so it has no embeddings to search on
                continue;
            }
            EmbeddingList embList = new EmbeddingList();
            for (Map<String, Object> struct : structs) {
                embList.add(converter.apply(struct));
            }
            searchData.add(embList);
        }

        search(annsField, searchData);
    }

    private static void searchFloatVectorField(String filter) {
        embeddingListSearch(
                FLOAT_VECTOR_FIELD,
                filter,
                struct -> new FloatVec((List<Float>) struct.get(FLOAT_VECTOR_FIELD)),
                "Search on float field '" + FLOAT_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
    }

    private static void searchBinaryVectorField(String filter) {
        embeddingListSearch(
                BINARY_VECTOR_FIELD,
                filter,
                struct -> new BinaryVec((ByteBuffer) struct.get(BINARY_VECTOR_FIELD)),
                "Search on binary field '" + BINARY_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
    }

    private static void searchInt8VectorField(String filter) {
        embeddingListSearch(
                INT8_VECTOR_FIELD,
                filter,
                struct -> new Int8Vec((ByteBuffer) struct.get(INT8_VECTOR_FIELD)),
                "Search on int8 field '" + INT8_VECTOR_FIELD + "' in struct '" + STRUCT_FIELD + "'"
        );
    }

    private static void elementLevelSearch() {
        System.out.println("===================================================");
        System.out.println("Element-level search on field '" + FLOAT_VECTOR_FIELD + "' in struct '" + SIMPLIFY_STRUCT_FIELD + "'");

        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " == 5")
                .limit(1)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Collections.singletonList(SIMPLIFY_STRUCT_FIELD))
                .build());
        List<Map<String, Object>> structs = (List<Map<String, Object>>) queryResp.getQueryResults().get(0).getEntity().get(SIMPLIFY_STRUCT_FIELD);

        // each row of SIMPLIFY_STRUCT_FIELD contains two structs, and each struct has a FLOAT_VECTOR_FIELD,
        // so we will have 2 vectors to search for each row
        List<BaseVector> elementSearchList = new ArrayList<>();
        for (Map<String, Object> struct : structs) {
            elementSearchList.add(new FloatVec((List<Float>) struct.get(FLOAT_VECTOR_FIELD)));
        }

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(String.format("%s[%s]", SIMPLIFY_STRUCT_FIELD, FLOAT_VECTOR_FIELD))
                .data(elementSearchList)
                .limit(3)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Collections.singletonList(NAME_FIELD))
                .build());
        // there will be two lists of search results corresponding to the two vectors in SIMPLIFY_STRUCT_FIELD,
        // and each search result will have an additional field "elementOffset" indicating which element
        // in the array field SIMPLIFY_STRUCT_FIELD this result corresponds to
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (int i = 0; i < searchResults.size(); i++) {
            System.out.println("Results of No." + i + " query element");
            for (SearchResp.SearchResult result : searchResults.get(i)) {
                System.out.println(result + ", elementOffset=" + result.getElementOffset());
            }
        }
    }

    private static void elementLevelQuery() {
        System.out.println("===================================================");
        String filter = String.format("element_filter(%s, $[%s] < 2000)", STRUCT_FIELD, FRAME_FIELD);
        System.out.println("Element-level query with filter expression: " + filter);

        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(filter)
                .limit(5)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Arrays.asList(ID_FIELD, NAME_FIELD,
                        String.format("%s[%s]", STRUCT_FIELD, FRAME_FIELD),
                        String.format("%s[%s]", STRUCT_FIELD, DESC_FIELD)))
                .build());
        // the server evaluates the filter at element granularity and returns element_indices;
        // the SDK expands one entity into one result per matched element, and each result carries
        // elementOffset (the matched element's index within the struct-array field). The source
        // entity index before expansion is intentionally not exposed: callers identify the source
        // entity by its primary key in the entity map.
        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result + ", elementOffset=" + result.getElementOffset());
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

        JsonObject newRow = buildBaseRow(EXTRA_ROW_ID, true);
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

    // Evolve an existing struct field: add sub-fields (including a nested Array sub-field), insert
    // rows carrying them, and drop one again. Sub-field add/drop use the bracketed `struct[sub]` path.
    private static void evolveStructSubField() {
        System.out.println("===================================================");
        System.out.println("Evolve struct field '" + STRUCT_FIELD + "' by adding sub-fields");

        client.addCollectionField(AddCollectionFieldReq.builder()
                .collectionName(COLLECTION_NAME)
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, NOTE_FIELD))
                .dataType(DataType.VarChar)
                .maxLength(64)
                .isNullable(true)
                .build());
        System.out.println("Added struct sub-field " + STRUCT_FIELD + "[" + NOTE_FIELD + "]");

        client.addCollectionField(AddCollectionFieldReq.builder()
                .collectionName(COLLECTION_NAME)
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, MARKS_FIELD))
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .maxCapacity(4)
                .isNullable(true)
                .elementNullable(true)
                .build());
        System.out.println("Added nested Array struct sub-field " + STRUCT_FIELD + "[" + MARKS_FIELD + "]");

        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, 6000L);
        row.addProperty(NAME_FIELD, "film_6000");
        JsonArray clips6000 = new JsonArray();
        JsonObject c0 = new JsonObject();
        c0.addProperty(FRAME_FIELD, 0);
        c0.addProperty(DESC_FIELD, "clip_description_6000");
        c0.add(FLOAT_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateFloatVector(FLOAT_VECTOR_DIM)));
        c0.add(BINARY_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateBinaryVector(BINARY_VECTOR_DIM).array()));
        c0.add(INT8_VECTOR_FIELD, JsonUtils.toJsonTree(CommonUtils.generateInt8Vector(INT8_VECTOR_DIM).array()));
        c0.addProperty(SCORE_FIELD, 7L);
        c0.addProperty(NOTE_FIELD, "hello");
        JsonArray marks0 = new JsonArray();
        marks0.add(1);
        marks0.add(2);
        c0.add(MARKS_FIELD, marks0);
        clips6000.add(c0);
        row.add(STRUCT_FIELD, clips6000);
        row.add(SIMPLIFY_STRUCT_FIELD, new JsonArray());
        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(row))
                .build());

        DescribeCollectionResp described = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection fields after sub-field evolution:");
        for (CreateCollectionReq.StructFieldSchema struct : described.getCollectionSchema().getStructFields()) {
            if (struct.getName().equals(STRUCT_FIELD)) {
                for (CreateCollectionReq.FieldSchema subField : struct.getFields()) {
                    System.out.printf("  %s: %s, isNullable=%s, elementNullable=%s%n",
                            subField.getName(), subField.getDataType(), subField.getIsNullable(), subField.getElementNullable());
                }
            }
        }

        client.dropCollectionField(DropCollectionFieldReq.builder()
                .collectionName(COLLECTION_NAME)
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, NOTE_FIELD))
                .build());
        System.out.println("Dropped struct sub-field " + STRUCT_FIELD + "[" + NOTE_FIELD + "]");
    }

    private static void verifyNullableStruct() {
        System.out.println("===================================================");
        System.out.println("Verify nullable struct field '" + STRUCT_FIELD + "' returns null data");

        // rows with id % 100 == 0 were inserted with clips = null
        List<Long> nullIds = Arrays.asList(0L, 100L, 200L, 300L);
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " in " + nullIds)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Arrays.asList(ID_FIELD, NAME_FIELD, STRUCT_FIELD, SIMPLIFY_STRUCT_FIELD))
                .build());
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            long id = (Long) result.getEntity().get(ID_FIELD);
            Object clips = result.getEntity().get(STRUCT_FIELD);
            Object simplifyClips = result.getEntity().get(SIMPLIFY_STRUCT_FIELD);
            System.out.println("query result id=" + id + ", clips=" + clips
                    + ", simplify_clips.size=" + ((List<?>) simplifyClips).size());
            if (id % 100 == 0 && clips != null) {
                throw new IllegalStateException("Expected null clips for id=" + id + ", got: " + clips);
            }
        }

        // take the simplify_clips vector of a null-clips row and search on it element-level;
        // the top-1 hit should be that row itself and its clips output must decode back to null
        QueryResp vectorQuery = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " == 100")
                .consistencyLevel(ConsistencyLevel.STRONG)
                .outputFields(Collections.singletonList(SIMPLIFY_STRUCT_FIELD))
                .build());
        List<Map<String, Object>> simplifyStructs = (List<Map<String, Object>>)
                vectorQuery.getQueryResults().get(0).getEntity().get(SIMPLIFY_STRUCT_FIELD);
        List<Float> queryVector = (List<Float>) simplifyStructs.get(0).get(FLOAT_VECTOR_FIELD);

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(String.format("%s[%s]", SIMPLIFY_STRUCT_FIELD, FLOAT_VECTOR_FIELD))
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .limit(5)
                .metricType(IndexParam.MetricType.L2)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .outputFields(Arrays.asList(ID_FIELD, STRUCT_FIELD))
                .build());
        boolean foundNullRow = false;
        for (SearchResp.SearchResult result : searchResp.getSearchResults().get(0)) {
            long id = (Long) result.getEntity().get(ID_FIELD);
            Object clips = result.getEntity().get(STRUCT_FIELD);
            System.out.println("search result id=" + id + ", clips=" + clips);
            if (id == 100L) {
                if (clips != null) {
                    throw new IllegalStateException("Expected null clips for id=100 in search result, got: " + clips);
                }
                foundNullRow = true;
            }
        }
        if (!foundNullRow) {
            throw new IllegalStateException("The null-clips row id=100 was not returned by search");
        }

        System.out.println("Nullable struct verification passed");
    }

    public static void main(String[] args) {
        try {
            createCollection();
            insertData(2000);

            // insert rows demonstrating element-level null and null struct rows
            List<JsonObject> elementNullRows = buildElementNullRows();
            client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(elementNullRows)
                    .build());
            verifyElementNull();

            verifyNullableStruct();

            searchFloatVectorField(ID_FIELD + " in [5, 8]");

            String filter1 = String.format("MATCH_LEAST(%s, $[%s] < 2000, threshold=3)", STRUCT_FIELD, FRAME_FIELD);
            searchBinaryVectorField(filter1);

            searchInt8VectorField(ID_FIELD + " == 999");
            elementLevelSearch();
            elementLevelQuery();

            addCollectionStructField();
            evolveStructSubField();

            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .build());
            System.out.println("Collection dropped: " + COLLECTION_NAME);
        } finally {
            client.close();
        }
    }
}

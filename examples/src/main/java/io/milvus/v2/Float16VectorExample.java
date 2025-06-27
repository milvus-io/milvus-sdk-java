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
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BFloat16Vec;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.nio.ByteBuffer;
import java.util.*;

public class Float16VectorExample {
    private static final String COLLECTION_NAME = "java_sdk_example_float16_vector_v2";
    private static final String ID_FIELD = "id";
    private static final String FP16_VECTOR_FIELD = "fp16_vector";
    private static final String BF16_VECTOR_FIELD = "bf16_vector";
    private static final Integer VECTOR_DIM = 128;

    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void createCollection() {

        // Drop the collection if you don't need the collection anymore
        Boolean has = client.hasCollection(HasCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        if (has) {
            dropCollection();
        }

        // Build a collection with two vector fields
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(FP16_VECTOR_FIELD)
                .dataType(io.milvus.v2.common.DataType.Float16Vector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(BF16_VECTOR_FIELD)
                .dataType(io.milvus.v2.common.DataType.BFloat16Vector)
                .dimension(VECTOR_DIM)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 64);
        indexes.add(IndexParam.builder()
                .fieldName(FP16_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName(BF16_VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
    }

    private static void prepareData(int count) {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            // The method for converting float32 vector to float16 vector can be found in
            // CommonUtils.
            ByteBuffer buf1 = CommonUtils.generateFloat16Vector(VECTOR_DIM, false);
            row.add(FP16_VECTOR_FIELD, gson.toJsonTree(buf1.array()));
            ByteBuffer buf2 = CommonUtils.generateFloat16Vector(VECTOR_DIM, true);
            row.add(BF16_VECTOR_FIELD, gson.toJsonTree(buf2.array()));
            rows.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        System.out.println(insertResp.getInsertCnt() + " rows inserted");
    }

    private static void searchVectors(List<Long> taargetIDs, List<BaseVector> targetVectors, String vectorFieldName) {
        int topK = 5;
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(targetVectors)
                .annsField(vectorFieldName)
                .limit(topK)
                .outputFields(Collections.singletonList(vectorFieldName))
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        if (searchResults.isEmpty()) {
            throw new RuntimeException("The search result is empty");
        }

        for (int i = 0; i < searchResults.size(); i++) {
            List<SearchResp.SearchResult> results = searchResults.get(i);
            if (results.size() != topK) {
                throw new RuntimeException(String.format("The search result should contains top%d items", topK));
            }

            SearchResp.SearchResult topResult = results.get(0);
            long id = (long) topResult.getId();
            if (id != taargetIDs.get(i)) {
                throw new RuntimeException("The top1 id is incorrect");
            }
            Map<String, Object> entity = topResult.getEntity();
            ByteBuffer vectorBuf = (ByteBuffer) entity.get(vectorFieldName);
            ByteBuffer targetVectorBuf = (ByteBuffer)targetVectors.get(i).getData();
            if (!vectorBuf.equals(targetVectorBuf)) {
                throw new RuntimeException("The top1 output vector is incorrect");
            }
            List<Float> decodedTargetVector = CommonUtils.decodeFloat16Vector(targetVectorBuf,
                    BF16_VECTOR_FIELD.equals(vectorFieldName));
            // The method for converting float16 vector to float32 vector can be found in
            // CommonUtils.
            List<Float> decodedFpVector = CommonUtils.decodeFloat16Vector(vectorBuf,
                    BF16_VECTOR_FIELD.equals(vectorFieldName));
            if (decodedFpVector.size() != VECTOR_DIM) {
                throw new RuntimeException("The decoded vector dimension is incorrect");
            }
            System.out.println("\nTarget vector: " + decodedTargetVector);
            System.out.println("Top0 result: " + topResult);
            System.out.println("Top0 result vector: " + decodedFpVector);
        }
        System.out.println("Search result of " + vectorFieldName + " is correct");
    }

    private static void search() {
        // Retrieve some rows for search
        List<Long> targetIDs = Arrays.asList(999L, 2024L);
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(ID_FIELD + " in " + targetIDs)
                .outputFields(Arrays.asList(FP16_VECTOR_FIELD, BF16_VECTOR_FIELD))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        if (queryResults.isEmpty()) {
            throw new RuntimeException("The query result is empty");
        }

        List<BaseVector> targetFP16Vectors = new ArrayList<>();
        List<BaseVector> targetBF16Vectors = new ArrayList<>();
        for (QueryResp.QueryResult queryResult : queryResults) {
            Map<String, Object> entity = queryResult.getEntity();
            ByteBuffer f16VectorBuf = (ByteBuffer) entity.get(FP16_VECTOR_FIELD);
            targetFP16Vectors.add(new Float16Vec(f16VectorBuf));
            ByteBuffer bf16VectorBuf = (ByteBuffer) entity.get(BF16_VECTOR_FIELD);
            targetBF16Vectors.add(new BFloat16Vec(bf16VectorBuf));
        }

        // Search float16 vector
        searchVectors(targetIDs, targetFP16Vectors, FP16_VECTOR_FIELD);

        // Search bfloat16 vector
        searchVectors(targetIDs, targetBF16Vectors, BF16_VECTOR_FIELD);
    }

    private static void dropCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection dropped");
    }

    public static void main(String[] args) {
        createCollection();
        prepareData(10000);
        search();
        dropCollection();

        client.close();
    }
}

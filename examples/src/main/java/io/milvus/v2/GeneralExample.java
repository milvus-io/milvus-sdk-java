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
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.DeleteResp;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class GeneralExample {
    private static final MilvusClientV2 client;

    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_general_v2";
    private static final String ID_FIELD = "userID";
    private static final String VECTOR_FIELD = "userFace";
    private static final Integer VECTOR_DIM = 64;
    private static final String AGE_FIELD = "userAge";

    private static final String INDEX_NAME = "userFaceIndex";
    private static final IndexParam.IndexType INDEX_TYPE = IndexParam.IndexType.IVF_FLAT;

    private static final Integer SEARCH_K = 5;

    private static void createCollection() {
        System.out.println("========== createCollection() ==========");
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.VarChar)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(Boolean.TRUE)
                .maxLength(100)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(AGE_FIELD)
                .dataType(DataType.Int8)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 128);
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexName(INDEX_NAME)
                .indexType(INDEX_TYPE)
                .metricType(IndexParam.MetricType.COSINE)
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
    }

    private static void describeCollection() {
        System.out.println("========== describeCollection() ==========");
        DescribeCollectionResp resp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println(resp);
    }

    private static void loadCollection() {
        System.out.println("========== loadCollection() ==========");
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
    }

    private static void releaseCollection() {
        System.out.println("========== releaseCollection() ==========");
        client.releaseCollection(ReleaseCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
    }

    private static void listCollections() {
        System.out.println("========== listCollections() ==========");
        ListCollectionsResp resp = client.listCollections();
        System.out.println(resp.getCollectionNames());
    }

    private static void createPartition(String partitionName) {
        System.out.println("========== createPartition() ==========");
        client.createPartition(CreatePartitionReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(partitionName)
                .build());
    }

    private static void listPartitions() {
        System.out.println("========== listCollections() ==========");
        List<String> partitions = client.listPartitions(ListPartitionsReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println(partitions);
    }

    private static List<Object> insertRows(String partitionName, int count) {
        System.out.println("========== insertRows() ==========");

        List<JsonObject> rows = new ArrayList<>();
        Random ran = new Random();
        Gson gson = new Gson();
        for (long i = 0; i < count; i++) {
            JsonObject row = new JsonObject();
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            row.addProperty(AGE_FIELD, ran.nextInt(99));
            rows.add(row);
        }

        InsertResp resp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .partitionName(partitionName)
                .data(rows)
                .build());
        // server returns id list if the primary key field is auto-id, the length of ids
        // is equal to resp.getInsertCnt()
        List<Object> ids = resp.getPrimaryKeys();
        System.out.println("complete insertRows, insertCount:" + ids.size());
        return ids;
    }

    private static void deleteByIds(List<Object> ids) {
        System.out.println("========== deleteByIds() ==========");
        DeleteResp resp = client.delete(DeleteReq.builder()
                .collectionName(COLLECTION_NAME)
                .ids(ids)
                .build());
        System.out.println("Deleted " + resp.getDeleteCnt() + " rows by ids");
    }

    private static void deleteByFilter(String filter) {
        System.out.println("========== deleteByFilter() ==========");
        DeleteResp resp = client.delete(DeleteReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(filter)
                .build());
        System.out.println("Deleted " + resp.getDeleteCnt() + " rows by filter: " + filter);
    }

    private static void truncateCollection() {
        System.out.println("========== truncateCollection() ==========");
        client.truncateCollection(TruncateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection '" + COLLECTION_NAME + "' truncated");
    }

    private static void renameCollection() {
        System.out.println("========== renameCollection() ==========");
        String newName = COLLECTION_NAME + "_renamed";
        client.renameCollection(RenameCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .newCollectionName(newName)
                .build());
        System.out.println("Collection '" + COLLECTION_NAME + "' renamed to '" + newName + "'");

        // Describe the collection under its new name to confirm the rename took effect.
        DescribeCollectionResp resp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(newName)
                .build());
        System.out.println("Describe renamed collection: " + resp.getCollectionName());

        // Rename it back so the rest of the example keeps using the original name.
        client.renameCollection(RenameCollectionReq.builder()
                .collectionName(newName)
                .newCollectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection '" + newName + "' renamed back to '" + COLLECTION_NAME + "'");
    }

    private static void searchFace(String filter) {
        System.out.println("========== searchFace() ==========");

        List<String> outputFields = Collections.singletonList(AGE_FIELD);
        List<BaseVector> vectors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            vectors.add(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM)));
        }

        long begin = System.currentTimeMillis();
        Map<String, Object> params = new HashMap<>();
        params.put("nprobe", 10);
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .limit(SEARCH_K)
                .data(vectors)
                .annsField(VECTOR_FIELD)
                .filter(filter)
                .searchParams(params)
                .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                .outputFields(outputFields)
                .build());

        long end = System.currentTimeMillis();
        long cost = (end - begin);
        System.out.println("Search time cost: " + cost + "ms");

        List<List<SearchResp.SearchResult>> searchResults = resp.getSearchResults();
        int i = 0;
        for (List<SearchResp.SearchResult> results : searchResults) {
            System.out.println("Search result of No." + i++);
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }

    public static void main(String[] args) {
        createCollection();
        describeCollection();
        listCollections();

        final String partitionName = "p1";
        createPartition(partitionName);
        listPartitions();

        // Insert a small batch so the delete/truncate demos operate on a manageable row count.
        List<Object> ids = insertRows(partitionName, 1000);

        loadCollection();

        deleteByIds(ids.subList(0, 100));
        deleteByFilter(AGE_FIELD + " > 90");

        String searchExpr = AGE_FIELD + " > 50";
        searchFace(searchExpr);

        truncateCollection();

        renameCollection();

        releaseCollection();
    }
}
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
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.UpsertResp;

import java.util.*;

public class UpsertExample {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_upsert_v2";
    private static final String ID_FIELD = "pk";
    private static final String VECTOR_FIELD = "vector";
    private static final String TEXT_FIELD = "text";
    private static final Integer VECTOR_DIM = 128;

    private static List<Object> createCollection(boolean autoID) {
        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(autoID)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(TEXT_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
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
        System.out.println("\nCollection created with autoID = " + autoID);

        // Insert rows
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            if (!autoID) {
                row.addProperty(ID_FIELD, i);
            }
            List<Float> vector = CommonUtils.generateFloatVector(VECTOR_DIM);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector));
            row.addProperty(TEXT_FIELD, String.format("text_%d", i));
            rows.add(row);
        }
        InsertResp resp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        return resp.getPrimaryKeys();
    }

    private static void queryWithExpr(String expr) {
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(expr)
                .outputFields(Arrays.asList(ID_FIELD, TEXT_FIELD))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResp.QueryResult> records = queryRet.getQueryResults();
        for (QueryResp.QueryResult record : records) {
            System.out.println(record.getEntity());
        }
    }

    private static void doUpsert(boolean autoID) {
        // if autoID is true, the collection primary key is auto-generated by server
        List<Object> ids = createCollection(autoID);

        // query before upsert
        Long testID = (Long)ids.get(1);
        String filter = String.format("%s == %d", ID_FIELD, testID);
        queryWithExpr(filter);

        // upsert
        // the server will return a new primary key, the old entity is deleted,
        // and a new entity is created with the new primary key
        Gson gson = new Gson();
        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, testID);
        List<Float> vector = CommonUtils.generateFloatVector(VECTOR_DIM);
        row.add(VECTOR_FIELD, gson.toJsonTree(vector));
        row.addProperty(TEXT_FIELD, "this field has been updated");
        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(row))
                .build());
        List<Object> newIds = upsertResp.getPrimaryKeys();
        Long newID = (Long)newIds.get(0);
        System.out.println("\nUpsert done");

        // query after upsert
        filter = String.format("%s == %d", ID_FIELD, newID);
        queryWithExpr(filter);
    }

    public static void main(String[] args) {
        doUpsert(true);
        doUpsert(false);

        client.close();
    }
}

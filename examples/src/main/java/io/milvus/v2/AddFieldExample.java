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
import io.milvus.v2.service.collection.request.AddCollectionFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AddFieldExample {
    private static final MilvusClientV2 client;

    static {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        client = new MilvusClientV2(config);
    }

    private static final String COLLECTION_NAME = "java_sdk_example_add_field_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 8;

    private static void createCollection() {
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
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
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
        System.out.println("Collection created");
    }

    private static void query(long id) {
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .ids(Collections.singletonList(id))
                .outputFields(Collections.singletonList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("\nQuery with id: " + id);
        List<QueryResp.QueryResult> records = queryRet.getQueryResults();
        for (QueryResp.QueryResult record : records) {
            System.out.println(record.getEntity());
        }
        System.out.println("=============================================================");
    }

    public static void main(String[] args) {
        createCollection();

        Gson gson = new Gson();
        {
            // Insert one row
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, 100L);
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(row))
                    .build());
        }

        {
            // Add a new field, the new field must be nullable, for the previous row, the "text" value is null
            client.addCollectionField(AddCollectionFieldReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .fieldName("text")
                    .dataType(DataType.VarChar)
                    .maxLength(100)
                    .isNullable(true)
                    .build());

            // Query the previous row
            query(100L);
        }

        {
            // Add a new row
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, 500L);
            row.addProperty("text", "this is a new row");
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(row))
                    .build());

            // Query the new row
            query(500L);
        }

        client.close();
    }
}

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

package io.milvus.v1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.QueryResults;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.UpsertParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.QueryResultsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class UpsertExample {
    private static final MilvusClient client;

    static {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        client = new MilvusServiceClient(connectParam);
    }

    private static final String COLLECTION_NAME = "java_sdk_example_upsert_v1";
    private static final String ID_FIELD = "pk";
    private static final String VECTOR_FIELD = "vector";
    private static final String TEXT_FIELD = "text";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(String expr) {
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr)
                .withOutFields(Arrays.asList(ID_FIELD, TEXT_FIELD))
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResultsWrapper.RowRecord> records = queryWrapper.getRowRecords();
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
        }
    }

    private static List<Long> createCollection(boolean autoID) {
        // Define fields
        List<FieldType> fieldsSchema = Arrays.asList(
                FieldType.newBuilder()
                        .withName(ID_FIELD)
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(autoID)
                        .build(),
                FieldType.newBuilder()
                        .withName(VECTOR_FIELD)
                        .withDataType(DataType.FloatVector)
                        .withDimension(VECTOR_DIM)
                        .build(),
                FieldType.newBuilder()
                        .withName(TEXT_FIELD)
                        .withDataType(DataType.VarChar)
                        .withMaxLength(100)
                        .build()
        );

        CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder()
                .withFieldTypes(fieldsSchema)
                .build();

        // Drop the collection if exists
        client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        // Create the collection with 3 fields
        R<RpcStatus> ret = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withSchema(collectionSchemaParam)
                .build());
        CommonUtils.handleResponseStatus(ret);

        // Specify an index type on the vector field.
        ret = client.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build());
        CommonUtils.handleResponseStatus(ret);

        // Call loadCollection() to enable automatically loading data into memory for searching
        client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
        System.out.println("\nCollection created with autoID = " + autoID);

        // insert rows
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
        R<MutationResult> resp = client.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        CommonUtils.handleResponseStatus(resp);
        return resp.getData().getIDs().getIntId().getDataList();
    }

    private static void doUpsert(boolean autoID) {
        // if autoID is true, the collection primary key is auto-generated by server
        List<Long> ids = createCollection(autoID);

        // query before upsert
        Long testID = ids.get(1);
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
        R<MutationResult> upsertResp = client.upsert(UpsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(Collections.singletonList(row))
                .build());
        CommonUtils.handleResponseStatus(upsertResp);
        List<Long> newIds = upsertResp.getData().getIDs().getIntId().getDataList();
        Long newID = newIds.get(0);
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

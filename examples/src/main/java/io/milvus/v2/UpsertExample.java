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
    private static final String TEXT_FIELD = "text_field";
    private static final String JSON_FIELD = "json_field";
    private static final String NULLABLE_FIELD = "nullable_field";
    private static final Integer VECTOR_DIM = 4;

    private static List<Object> createCollection(boolean autoID) {
        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
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
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(JSON_FIELD)
                .dataType(DataType.JSON)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(NULLABLE_FIELD)
                .dataType(DataType.Int32)
                .isNullable(true)
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
            JsonObject metadata = new JsonObject();
            metadata.addProperty("foo", i);
            metadata.addProperty("bar", i);
            row.add(JSON_FIELD, metadata);
            row.addProperty(NULLABLE_FIELD, i);
            row.addProperty("dynamic", String.format("dynamic_%d", i)); // this is dynamic field
            rows.add(row);
        }
        InsertResp resp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        return resp.getPrimaryKeys();
    }

    private static List<QueryResp.QueryResult> queryWithExpr(String expr) {
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(expr)
                .outputFields(Collections.singletonList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("Query with expression: " + expr);
        List<QueryResp.QueryResult> records = queryRet.getQueryResults();
        for (QueryResp.QueryResult record : records) {
            System.out.println(record.getEntity());
        }
        return records;
    }

    // update the entire row
    private static void fullUpsert(Object id) {
        System.out.println("------------------------------ full upsert ------------------------------");
        Gson gson = new Gson();
        // Query before upsert, get the No.2 primary key
        String filter = String.format("%s == %s", ID_FIELD, id);
        queryWithExpr(filter);

        // Upsert, update all fields value
        // If autoID is true, the server will return a new primary key for the updated entity
        JsonObject row = new JsonObject();
        row.addProperty(ID_FIELD, (Long) id); // primary key must be input so that it can know which entity to be updated
        List<Float> vectorUpdated = Arrays.asList(1.0f, 1.0f, 1.0f, 1.0f);
        row.add(VECTOR_FIELD, gson.toJsonTree(vectorUpdated));
        String textUpdated = "this field has been updated";
        row.addProperty(TEXT_FIELD, textUpdated);
        JsonObject metadata = new JsonObject();
        metadata.addProperty("updated", "yes");
        row.add(JSON_FIELD, metadata); // the json field will be overridden
        row.add(NULLABLE_FIELD, null); // update nullable field to null
        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(row))
                .build());
        List<Object> newIds = upsertResp.getPrimaryKeys();
        System.out.printf("\nUpsert done, primary key %s has been updated to %s%n", id, newIds.get(0));

        // Query after upsert, you will see the vector field is [1.0f, 1.0f, 1.0f, 1.0f],
        // text field is "this field has been updated", nullable field is null
        filter = String.format("%s == %s", ID_FIELD, newIds.get(0));
        List<QueryResp.QueryResult> results = queryWithExpr(filter);

        // Verify the result
        if (results.size() != newIds.size()) {
            throw new RuntimeException("Incorrect query result for filter: " + filter);
        }
        Map<String, Object> entity = results.get(0).getEntity();
        if (!vectorUpdated.equals(entity.get(VECTOR_FIELD))) {
            throw new RuntimeException("Vector field is not correctly updated for filter: " + filter);
        }
        if (!textUpdated.equals(entity.get(TEXT_FIELD))) {
            throw new RuntimeException("Text field is not correctly updated for filter: " + filter);
        }
        // In full upsert, JSON field is overridden
        if (!entity.get(JSON_FIELD).equals(metadata)) {
            throw new RuntimeException("JSON field is not correctly updated for filter: " + filter);
        }
        if (entity.get(NULLABLE_FIELD) != null) {
            throw new RuntimeException("Nullable field is not correctly updated for filter: " + filter);
        }
        // Note that we didn't input the dynamic field for full update, so it will treat it as removed
        if (entity.containsKey("dynamic")) {
            throw new RuntimeException("Dynamic field is not removed for filter: " + filter);
        }
    }

    // update the specified field, other fields will keep old values
    private static void partialUpsert(List<Object> ids, boolean updateVector) {
        System.out.printf("\n------------------------------ partial upsert %s ------------------------------%n",
                updateVector ? "vector" : "scalars");
        Gson gson = new Gson();
        // Query before upsert
        String filter = String.format("%s in %s", ID_FIELD, ids);
        List<QueryResp.QueryResult> oldResults = queryWithExpr(filter);

        // Partial upsert, only update the specified field, other fields will keep old values
        // If autoID is true, the server will return a new primary key for the updated entity
        // Note: for the case to do partial upsert for multi entities, it only allows updating
        // the same fields for all rows.
        // For example, assume a collection has 2 fields: A and B
        // it is legal to update the same fields as: client.upsert(data = [ {"A": 1}, {"A": 3}])
        // it is illegal to update different fields as: client.upsert(data = [ {"A": 1}, {"B": 3}])
        // Read the doc for more info: https://milvus.io/docs/upsert-entities.md
        List<Float> vectorUpdated = Arrays.asList(1.0f, 1.0f, 1.0f, 1.0f);
        String textUpdated = "this row has been partially updated";
        List<JsonObject> rows = new ArrayList<>();
        for (Object id : ids) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, (Long) id); // primary key must be input so that it can know which entity to be updated
            if (updateVector) {
                row.add(VECTOR_FIELD, gson.toJsonTree(vectorUpdated));
            } else {
                row.addProperty(TEXT_FIELD, textUpdated);
                row.add(NULLABLE_FIELD, null);
                JsonObject metadata = new JsonObject();
                metadata.addProperty("updated", "yes");
                row.add(JSON_FIELD, metadata); // the json field will be merged in partial upsert
            }
            row.addProperty("new_dynamic", "new"); // add a new dynamic field
            rows.add(row);
        }

        UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .partialUpdate(true)
                .build());
        List<Object> newIds = upsertResp.getPrimaryKeys();
        System.out.printf("\nPartial upsert done, primary keys %s has been updated to %s%n", ids, newIds);

        // query after upsert, you will see the text field is "this row has been partially updated"
        // the other fields keep old values
        filter = String.format("%s in %s", ID_FIELD, newIds);
        List<QueryResp.QueryResult> results = queryWithExpr(filter);

        // Verify the result
        if (results.size() != newIds.size()) {
            throw new RuntimeException("Incorrect query result for filter: " + filter);
        }

        for (int i = 0; i < results.size(); i++) {
            Map<String, Object> oldEntity = oldResults.get(i).getEntity();
            Map<String, Object> entity = results.get(i).getEntity();
            if (updateVector) {
                // only vector field is updated
                if (!vectorUpdated.equals(entity.get(VECTOR_FIELD))) {
                    throw new RuntimeException("Vector field is not correctly updated for filter: " + filter);
                }

                // the other fields keep old values
                if (!entity.get(TEXT_FIELD).equals(oldEntity.get(TEXT_FIELD))) {
                    throw new RuntimeException("Text field should not be updated for filter: " + filter);
                }
                if (!entity.get(JSON_FIELD).equals(oldEntity.get(JSON_FIELD))) {
                    throw new RuntimeException("JSON field should not be updated for filter: " + filter);
                }
                if (!entity.get(NULLABLE_FIELD).equals(oldEntity.get(NULLABLE_FIELD))) {
                    throw new RuntimeException("Nullable field should not be updated for filter: " + filter);
                }
            } else {
                // scalar fields are updated
                if (!textUpdated.equals(entity.get(TEXT_FIELD))) {
                    throw new RuntimeException("Text field is not correctly updated for filter: " + filter);
                }
                if (entity.get(NULLABLE_FIELD) != null) {
                    throw new RuntimeException("Nullable field is not correctly updated for filter: " + filter);
                }
                JsonObject newJson = (JsonObject) entity.get(JSON_FIELD);
                if (!newJson.has("updated") && !newJson.get("updated").equals("yes")) {
                    throw new RuntimeException("JSON field is not correctly updated for filter: " + filter);
                }

                // vector field keep old value
                if (!entity.get(VECTOR_FIELD).equals(oldEntity.get(VECTOR_FIELD))) {
                    throw new RuntimeException("Vector field should not be updated for filter: " + filter);
                }
            }
            // Note that we didn't input the dynamic field for partial update, it will keep old value
            if (!entity.get("dynamic").equals(oldEntity.get("dynamic"))) {
                throw new RuntimeException("Dynamic field should not be updated for filter: " + filter);
            }
            // Verify the new dynamic field is merged
            if (!entity.containsKey("new_dynamic") && !entity.get("new_dynamic").equals("new")) {
                throw new RuntimeException("New dynamic field is not merged for filter: " + filter);
            }
        }
    }

    private static void doUpsert(boolean autoID) {
        System.out.printf("\n================================== autoID = %s ==================================", autoID ? "true" : "false");
        // If autoID is true, the collection primary key is auto-generated by server
        List<Object> ids = createCollection(autoID);

        // Update the entire row of the No.2 entity
        fullUpsert((Long) ids.get(1));

        // Partially update the vectors of No.5 and No.6 entities
        partialUpsert(ids.subList(4, 6), true);

        // Partially update the scalar fields of No.10 entity
        partialUpsert(ids.subList(9, 10), false);
    }

    public static void main(String[] args) {
        doUpsert(true);
        doUpsert(false);

        client.close();
    }
}

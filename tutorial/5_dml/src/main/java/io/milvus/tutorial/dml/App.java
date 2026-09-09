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

package io.milvus.tutorial.dml;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.DeleteResp;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.UpsertResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tutorial 5: Data manipulation language (DML).
 *
 * <p>Demonstrates writing and modifying data with {@code MilvusClientV2}: insert rows, upsert to
 * replace or partially update an entity, delete by primary key or by filter, and verify the
 * remaining rows with {@code query}.
 *
 * <p>Connection defaults to {@code MILVUS_URI=http://localhost:19530} and
 * {@code MILVUS_TOKEN=root:Milvus}. Override them with environment variables when needed.
 */
public class App {
    private static final int DIMENSION = 4;

    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("MILVUS_URI", "http://localhost:19530");
        String token = System.getenv().getOrDefault("MILVUS_TOKEN", "root:Milvus");

        ConnectConfig config = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        try {
            String collectionName = "tutorial_dml";

            // Pre-drop any collection left behind by an interrupted run; dropping a non-existent
            // collection succeeds, so this keeps the tutorial rerunnable.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());

            createAndLoadCollection(client, collectionName);

            Gson gson = new Gson();

            // insert writes row-oriented JSON entities. Each row follows the collection schema;
            // the response reports how many entities were accepted.
            List<JsonObject> rows = new ArrayList<>();
            rows.add(row(gson, 1, "Java in Action", 35.0, new float[]{0.1f, 0.2f, 0.3f, 0.4f}));
            rows.add(row(gson, 2, "Vector Search", 25.0, new float[]{0.2f, 0.3f, 0.4f, 0.5f}));
            InsertResp insertResp = client.insert(InsertReq.builder()
                    .collectionName(collectionName)
                    .data(rows)
                    .build());
            System.out.printf("Inserted %d rows%n", insertResp.getInsertCnt());

            // upsert replaces an existing entity or inserts it when the primary key is absent.
            List<JsonObject> upsertRows = new ArrayList<>();
            upsertRows.add(row(gson, 2, "Practical Vector Search", 27.5, new float[]{0.25f, 0.35f, 0.45f, 0.55f}));
            upsertRows.add(row(gson, 3, "Milvus Guide", 30.0, new float[]{0.3f, 0.4f, 0.5f, 0.6f}));
            UpsertResp upsertResp = client.upsert(UpsertReq.builder()
                    .collectionName(collectionName)
                    .data(upsertRows)
                    .build());
            System.out.printf("Upserted %d rows%n", upsertResp.getUpsertCnt());

            // upsert with partialUpdate(true) changes only the supplied non-primary fields. The
            // primary key still identifies which entity to update.
            List<JsonObject> partialRows = new ArrayList<>();
            JsonObject partial = new JsonObject();
            partial.addProperty("id", 3);
            partial.addProperty("title", "The Milvus Guide");
            partialRows.add(partial);
            UpsertResp partialResp = client.upsert(UpsertReq.builder()
                    .collectionName(collectionName)
                    .data(partialRows)
                    .partialUpdate(true)
                    .build());
            System.out.printf("Partially updated %d rows%n", partialResp.getUpsertCnt());

            // delete with ids removes entities by primary key.
            DeleteResp deleteById = client.delete(DeleteReq.builder()
                    .collectionName(collectionName)
                    .ids(Arrays.asList(1))
                    .build());
            System.out.printf("Deleted %d rows by primary key%n", deleteById.getDeleteCnt());

            // delete with filter removes all entities matching the Milvus boolean expression.
            DeleteResp deleteByFilter = client.delete(DeleteReq.builder()
                    .collectionName(collectionName)
                    .filter("id >= 3")
                    .build());
            System.out.printf("Deleted %d rows by filter%n", deleteByFilter.getDeleteCnt());

            printRemainingRows(client, collectionName);

            // Clean up.
            client.dropCollection(DropCollectionReq.builder()
                    .collectionName(collectionName)
                    .build());
            System.out.printf("Collection '%s' dropped%n", collectionName);
        } finally {
            client.close();
        }
    }

    private static JsonObject row(Gson gson, int id, String title, double price, float[] vector) {
        JsonObject row = new JsonObject();
        row.addProperty("id", id);
        row.addProperty("title", title);
        row.addProperty("price", price);
        row.add("embedding", gson.toJsonTree(vector));
        return row;
    }

    private static void createAndLoadCollection(MilvusClientV2 client, String collectionName) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("title")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("price")
                .dataType(DataType.Float)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("embedding")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        // createCollection creates the DML target. The index makes the vector field searchable.
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .indexParams(Arrays.asList(IndexParam.builder()
                        .fieldName("embedding")
                        .indexType(IndexParam.IndexType.AUTOINDEX)
                        .build()))
                .build());
        System.out.printf("Collection '%s' created%n", collectionName);

        // loadCollection prepares the collection for the verification query.
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.println("Collection loaded");
    }

    private static void printRemainingRows(MilvusClientV2 client, String collectionName) {
        // query reads the remaining entities. Strong consistency makes the preceding mutations
        // visible to the query.
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("id >= 0")
                .outputFields(Arrays.asList("id", "title", "price"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.println("Remaining rows:");
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Map<String, Object> entity = result.getEntity();
            System.out.printf("  id=%s title=%s price=%s%n",
                    entity.get("id"), entity.get("title"), entity.get("price"));
        }
    }
}

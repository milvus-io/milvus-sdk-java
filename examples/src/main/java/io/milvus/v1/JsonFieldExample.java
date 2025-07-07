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
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.response.SearchResultsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JsonFieldExample {
    private static final String COLLECTION_NAME = "java_sdk_example_json_v1";
    private static final String ID_FIELD = "key";
    private static final String VECTOR_FIELD = "vector";
    private static final String JSON_FIELD = "metadata";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClient client, String expr) {
        System.out.printf("%n=============================Query with expr: '%s'================================%n", expr);
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr)
                .withOutFields(Arrays.asList(ID_FIELD, JSON_FIELD, "dynamic1", "dynamic2"))
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResultsWrapper.RowRecord> records = queryWrapper.getRowRecords();
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
        }
    }

    public static void main(String[] args) {
        // Connect to Milvus server. Replace the "localhost" and port with your Milvus server address.
        MilvusServiceClient client = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build());

        // Define fields
        List<FieldType> fieldsSchema = Arrays.asList(
                FieldType.newBuilder()
                        .withName(ID_FIELD)
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build(),
                FieldType.newBuilder()
                        .withName(VECTOR_FIELD)
                        .withDataType(DataType.FloatVector)
                        .withDimension(VECTOR_DIM)
                        .build(),
                FieldType.newBuilder()
                        .withName(JSON_FIELD)
                        .withDataType(DataType.JSON)
                        .build()
        );

        CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(true)
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
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage());
        }

        // Specify an index type on the vector field.
        ret = client.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create index on vector field! Error: " + ret.getMessage());
        }

        // Call loadCollection() to enable automatically loading data into memory for searching
        client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        System.out.println("Collection created");

        // insert rows
        List<List<Float>> vectors = new ArrayList<>();
        List<JsonObject> metadatas = new ArrayList<>();
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            List<Float> vector = CommonUtils.generateFloatVector(VECTOR_DIM);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector));
            vectors.add(vector);

            // Note: for JSON field, always construct a real JsonObject
            // don't use row.addProperty(JSON_FIELD, strContent) since the value is treated as a string, not a JsonObject
            JsonObject metadata = new JsonObject();
            metadata.addProperty("path", String.format("\\root/abc/path_%d", i));
            metadata.addProperty("size", i);
            if (i%7 == 0) {
                metadata.addProperty("special", true);
            }

            metadata.add("flags", gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.add(JSON_FIELD, metadata);
            metadatas.add(metadata);
//            System.out.println(metadata);

            // dynamic fields
            if (i%2 == 0) {
                row.addProperty("dynamic1", (double)i/3);
            } else {
                row.addProperty("dynamic2", "ok");
            }

            client.insert(InsertParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .withRows(Collections.singletonList(row))
                    .build());
        }

        // get row count
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr("")
                .addOutField("count(*)")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        long rowCount = (long)queryWrapper.getFieldWrapper("count(*)").getFieldData().get(0);
        System.out.printf("%d rows persisted\n", rowCount);

        // search and output JSON field
        List<List<Float>> searchVectors = new ArrayList<>();
        List<JsonObject> expectedMetadatas = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Float> targetVector = vectors.get(i);
            searchVectors.add(targetVector);
            expectedMetadatas.add(metadatas.get(i));
        }
        R<SearchResults> searchRet = client.search(SearchParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withLimit(3L)
                .withFloatVectors(searchVectors)
                .withVectorFieldName(VECTOR_FIELD)
                .addOutField(ID_FIELD)
                .addOutField(VECTOR_FIELD)
                .addOutField(JSON_FIELD)
                .build());
        CommonUtils.handleResponseStatus(searchRet);

        SearchResultsWrapper resultsWrapper = new SearchResultsWrapper(searchRet.getData().getResults());
        System.out.println("\n=============================Search result with IDScore================================");
        for (int i = 0; i < 10; i++) {
            List<SearchResultsWrapper.IDScore> scores = resultsWrapper.getIDScore(i);
            System.out.printf("\nThe result of No.%d target vector:\n", i);
            for (SearchResultsWrapper.IDScore score : scores) {
                System.out.println(score);
            }
            long pk = scores.get(0).getLongID();
            if (pk != i) {
                throw new RuntimeException(String.format("The top1 ID %d is not equal to target vector's ID %d", pk, i));
            }
            JsonObject metadata = (JsonObject) scores.get(0).get(JSON_FIELD);
            if (!metadata.equals(expectedMetadatas.get(i))) {
                throw new RuntimeException(String.format("The top1 metadata %s is not equal to target metadata %s",
                        metadata, expectedMetadatas.get(i)));
            }
            List<Float> vector = (List<Float>) scores.get(0).get(VECTOR_FIELD);
            CommonUtils.compareFloatVectors(vector, searchVectors.get(i));
        }
        System.out.println("\n=============================Search result with RowRecord================================");
        for (int i = 0; i < 10; i++) {
            List<QueryResultsWrapper.RowRecord> records = resultsWrapper.getRowRecords(i);
            System.out.printf("\nThe result of No.%d target vector:\n", i);
            for (QueryResultsWrapper.RowRecord record : records) {
                System.out.println(record);
            }
            long pk = (long)records.get(0).get(ID_FIELD);
            if (pk != i) {
                throw new RuntimeException(String.format("The top1 ID %d is not equal to target vector's ID %d", pk, i));
            }
            JsonObject metadata = (JsonObject) records.get(0).get(JSON_FIELD);
            if (!metadata.equals(expectedMetadatas.get(i))) {
                throw new RuntimeException(String.format("The top1 metadata %s is not equal to target metadata %s",
                        metadata, expectedMetadatas.get(i)));
            }
            List<Float> vector = (List<Float>) records.get(0).get(VECTOR_FIELD);
            CommonUtils.compareFloatVectors(vector, searchVectors.get(i));
        }

        // query by filtering JSON
        queryWithExpr(client, "exists metadata[\"special\"]");
        queryWithExpr(client, "metadata[\"size\"] < 5");
        queryWithExpr(client, "metadata[\"size\"] in [4, 5, 6]");
        queryWithExpr(client, "JSON_CONTAINS(metadata[\"flags\"], 9)");
        queryWithExpr(client, "JSON_CONTAINS_ANY(metadata[\"flags\"], [8, 9, 10])");
        queryWithExpr(client, "JSON_CONTAINS_ALL(metadata[\"flags\"], [8, 9, 10])");
        queryWithExpr(client, "dynamic1 < 2.0");
    }
}

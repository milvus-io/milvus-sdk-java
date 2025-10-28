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
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.DataType;
import io.milvus.grpc.QueryResults;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.QueryResultsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class ArrayFieldExample {
    private static final String COLLECTION_NAME = "java_sdk_example_array_v1";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClient client, String expr) {
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr)
                .addOutField("array_int32")
                .addOutField("array_varchar")
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResultsWrapper.RowRecord> records = queryWrapper.getRowRecords();
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
        }
        System.out.printf("%d items matched%n", records.size());
        System.out.println("=============================================================");
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
                        .withAutoID(true)
                        .build(),
                FieldType.newBuilder()
                        .withName(VECTOR_FIELD)
                        .withDataType(DataType.FloatVector)
                        .withDimension(VECTOR_DIM)
                        .build(),
                FieldType.newBuilder()
                        .withName("array_int32")
                        .withDataType(DataType.Array)
                        .withElementType(DataType.Int32)
                        .withMaxCapacity(10)
                        .build(),
                FieldType.newBuilder()
                        .withName("array_varchar")
                        .withDataType(DataType.Array)
                        .withElementType(DataType.VarChar)
                        .withMaxCapacity(10)
                        .withMaxLength(100)
                        .build()
        );

        CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(false)
                .withFieldTypes(fieldsSchema)
                .build();

        // Drop the collection if exists
        client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        // Create the collection
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

        // Insert data by column-based
        Random random = new Random();
        int rowCount = 100;
        List<List<Integer>> intArrArray = new ArrayList<>();
        List<List<String>> strArrArray = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            List<Integer> intArray = new ArrayList<>();
            List<String> strArray = new ArrayList<>();
            int capacity = random.nextInt(5) + 5;
            for (int k = 0; k < capacity; k++) {
                intArray.add((i + k) % 100);
                strArray.add(String.format("string-%d-%d", i, k));
            }
            intArrArray.add(intArray);
            strArrArray.add(strArray);
        }
        List<List<Float>> vectors = CommonUtils.generateFloatVectors(VECTOR_DIM, rowCount);

        List<InsertParam.Field> fieldsInsert = new ArrayList<>();
        fieldsInsert.add(new InsertParam.Field(VECTOR_FIELD, vectors));
        fieldsInsert.add(new InsertParam.Field("array_int32", intArrArray));
        fieldsInsert.add(new InsertParam.Field("array_varchar", strArrArray));

        InsertParam insertColumnsParam = InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFields(fieldsInsert)
                .build();

        client.insert(insertColumnsParam);
        System.out.println(rowCount + " rows inserted");

        // Insert rows
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            JsonObject row = new JsonObject();
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            row.add("array_int32", JsonUtils.toJsonTree(intArrArray.get(i)));
            row.add("array_varchar", JsonUtils.toJsonTree(strArrArray.get(i)));
            rows.add(row);
        }
        client.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        System.out.println(rowCount + " rows inserted");

        // Get row count
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr("")
                .addOutField("count(*)")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        System.out.printf("%d rows in collection\n", (long) queryWrapper.getFieldWrapper("count(*)").getFieldData().get(0));

        // Query by filtering expression
        queryWithExpr(client, "array_int32[0] == 99");
        queryWithExpr(client, "array_int32[1] in [5, 10, 15]");
        queryWithExpr(client, "array_varchar[0] like \"string-55%\"");
        queryWithExpr(client, "array_contains(array_varchar, \"string-4-1\")");
        queryWithExpr(client, "array_contains_any(array_int32, [3, 9])");
        queryWithExpr(client, "array_contains_all(array_int32, [3, 9])");
    }
}

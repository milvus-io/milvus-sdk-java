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
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.index.*;
import io.milvus.response.*;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;


public class SimpleExample {
    private static final String COLLECTION_NAME = "java_sdk_example_simple_v1";
    private static final String ID_FIELD = "book_id";
    private static final String VECTOR_FIELD = "book_intro";
    private static final String TITLE_FIELD = "book_title";
    private static final Integer VECTOR_DIM = 4;

    public static void main(String[] args) {
        // Connect to Milvus server. Replace the "localhost" and port with your Milvus server address.
        MilvusServiceClient milvusClient = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build());

        // set log level, only show errors
        milvusClient.setLogLevel(LogLevel.Error);

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
                        .withName(TITLE_FIELD)
                        .withDataType(DataType.VarChar)
                        .withMaxLength(64)
                        .build()
        );

        // Create the collection with 3 fields
        R<RpcStatus> ret = milvusClient.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldTypes(fieldsSchema)
                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage());
        }

        // Specify an index type on the vector field.
        ret = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create index on vector field! Error: " + ret.getMessage());
        }

        // Specify an index type on the varchar field.
        ret = milvusClient.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(TITLE_FIELD)
                .withIndexType(IndexType.TRIE)
                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create index on varchar field! Error: " + ret.getMessage());
        }

        // Call loadCollection() to enable automatically loading data into memory for searching
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        System.out.println("Collection created");

        // Insert 10 records into the collection
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 1L; i <= 10; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            List<Float> vector = Arrays.asList((float)i, (float)i, (float)i, (float)i);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector));
            row.addProperty(TITLE_FIELD, "Tom and Jerry " + i);
            rows.add(row);
        }

        R<MutationResult> insertRet = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        if (insertRet.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to insert! Error: " + insertRet.getMessage());
        }

        // get row count
        R<QueryResults> queryRet = milvusClient.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr("")
                .addOutField("count(*)")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        QueryResultsWrapper wrapper = new QueryResultsWrapper(queryRet.getData());
        long rowCount = (long)wrapper.getFieldWrapper("count(*)").getFieldData().get(0);
        System.out.printf("%d rows persisted\n", rowCount);

        // Construct a vector to search top5 similar records, return the book title for us.
        // This vector is equal to the No.3 record, we suppose the No.3 record is the most similar.
        List<Float> vector = Arrays.asList(3.0f, 3.0f, 3.0f, 3.0f);
        R<SearchResults> searchRet = milvusClient.search(SearchParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withMetricType(MetricType.L2)
                .withLimit(5L)
                .withFloatVectors(Collections.singletonList(vector))
                .withVectorFieldName(VECTOR_FIELD)
                .withParams("{}")
                .addOutField(VECTOR_FIELD)
                .addOutField(TITLE_FIELD)
                .build());
        if (searchRet.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to search! Error: " + searchRet.getMessage());
        }

        // The search() allows multiple target vectors to search in a batch.
        // Here we only input one vector to search, get the result of No.0 vector to print out
        SearchResultsWrapper resultsWrapper = new SearchResultsWrapper(searchRet.getData().getResults());
        List<SearchResultsWrapper.IDScore> scores = resultsWrapper.getIDScore(0);
        System.out.println("The result of No.0 target vector:");
        for (SearchResultsWrapper.IDScore score:scores) {
            List<Float> vectorReturned = (List<Float>)score.get(VECTOR_FIELD);
            System.out.println(vectorReturned);

            String title = (String)score.get(TITLE_FIELD);
            System.out.println(title);
        }

        // drop the collection if you don't need the collection anymore
        milvusClient.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());
    }
}
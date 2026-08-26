/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file
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
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.FunctionChain;
import io.milvus.v2.service.vector.request.FunctionChainArg;
import io.milvus.v2.service.vector.request.FunctionChainExpr;
import io.milvus.v2.service.vector.request.FunctionChainStage;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FunctionChainExample {
    private static final MilvusClientV2 client;

    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static final String COLLECTION_NAME = "java_sdk_example_function_chain_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static void buildCollection() {
        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create a collection with an int64 primary key and a float vector field
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

        List<IndexParam> indexes = Collections.singletonList(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .build());
        System.out.printf("Collection '%s' created%n", COLLECTION_NAME);

        // Load the collection before searching it
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Insert rows for the function-chain search
        long rowCount = 1000L;
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 0L; i < rowCount; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            rows.add(row);
        }

        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());
        System.out.printf("%d rows inserted%n", insertResp.getInsertCnt());

        // Flush so the freshly inserted data is searchable
        client.flush(FlushReq.builder()
                .collectionNames(Collections.singletonList(COLLECTION_NAME))
                .build());
    }

    private static void searchWithFunctionChain(int topK) {
        // Round each search score to two decimal places, sort descending, and return topK rows.
        FunctionChain functionChain = FunctionChain.builder()
                .stage(FunctionChainStage.L2_RERANK)
                .name("round_and_sort")
                .map("$score", FunctionChainExpr.builder()
                        .name("round_decimal")
                        .arg(FunctionChainArg.col("$score"))
                        .param("decimal", 2)
                        .build())
                .sort("$score", true, "")
                .limit(topK)
                .build();

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .annsField(VECTOR_FIELD)
                .data(Collections.singletonList(new FloatVec(
                        CommonUtils.generateFloatVector(VECTOR_DIM, 1.0f))))
                .outputFields(Collections.singletonList(ID_FIELD))
                .limit(topK)
                .addFunctionChain(functionChain)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        System.out.println("\nFunction-chain search results:");
        for (List<SearchResp.SearchResult> results : searchResp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("ID: %s, Score: %f%n", result.getId(), result.getScore());
            }
        }
    }

    public static void main(String[] args) {
        buildCollection();
        searchWithFunctionChain(10);
        client.close();
    }
}

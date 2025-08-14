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
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.Function;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class FullTextSearchExample {
    private static final String COLLECTION_NAME = "java_sdk_example_text_match_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";

    private static void searchByText(MilvusClientV2 client, String text) {
        // The text is tokenized inside server and turned into a sparse embedding to compare with the vector field
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(new EmbeddedText(text)))
                .limit(3)
                .outputFields(Collections.singletonList("text"))
                .build());
        System.out.println("\nSearch by text: " + text);
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
        System.out.println("=============================================================");
    }

    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .enableAnalyzer(true) // must enable this if you use Function
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.SparseFloatVector)
                .build());

        // With this function, milvus will convert the strings of "text" field to sparse vectors of "vector" field
        // by built-in tokenizer and analyzer
        // Read the link for more info: https://milvus.io/docs/full-text-search.md
        schema.addFunction(Function.builder()
                .functionType(FunctionType.BM25)
                .name("function_bm25")
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList(VECTOR_FIELD))
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25) // to use full text search, metric type must be "BM25"
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert rows
        Gson gson = new Gson();
        List<JsonObject> rows = Arrays.asList(
                gson.fromJson("{\"id\": 0, \"text\": \"Milvus is an open-source vector database\"}", JsonObject.class),
                gson.fromJson("{\"id\": 1, \"text\": \"AI applications help people better life\"}", JsonObject.class),
                gson.fromJson("{\"id\": 2, \"text\": \"Will the electric car replace gas-powered car?\"}", JsonObject.class),
                gson.fromJson("{\"id\": 3, \"text\": \"LangChain is a composable framework to build with LLMs. Milvus is integrated into LangChain.\"}", JsonObject.class),
                gson.fromJson("{\"id\": 4, \"text\": \"RAG is the process of optimizing the output of a large language model\"}", JsonObject.class),
                gson.fromJson("{\"id\": 5, \"text\": \"Newton is one of the greatest scientist of human history\"}", JsonObject.class),
                gson.fromJson("{\"id\": 6, \"text\": \"Metric type L2 is Euclidean distance\"}", JsonObject.class),
                gson.fromJson("{\"id\": 7, \"text\": \"Embeddings represent real-world objects, like words, images, or videos, in a form that computers can process.\"}", JsonObject.class),
                gson.fromJson("{\"id\": 8, \"text\": \"The moon is 384,400 km distance away from earth\"}", JsonObject.class),
                gson.fromJson("{\"id\": 9, \"text\": \"Milvus supports L2 distance and IP similarity for float vector.\"}", JsonObject.class)
        );

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows in collection\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // Query by filtering expression
        searchByText(client, "moon and earth distance");
        searchByText(client, "Milvus vector database");

        client.close();
    }
}

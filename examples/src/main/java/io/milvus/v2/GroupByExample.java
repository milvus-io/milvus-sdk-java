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
import io.milvus.v2.service.collection.request.CreateCollectionReq.Function;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.ranker.WeightedRanker;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class GroupByExample {
    private static final String COLLECTION_NAME = "java_sdk_example_group_by_v2";
    private static final String ID_FIELD = "id";
    private static final String DENSE_FIELD = "dense";
    private static final String SPARSE_FIELD = "sparse";
    private static final String TEXT_FIELD = "text";
    private static final String DOCID_FIELD = "docId";
    private static final int DIM = 5;

    private static void searchGroupBy(MilvusClientV2 client, String groupField, long limit, int groupSize, boolean strictGroup) {
        System.out.println("\n====================================================================================");
        BaseVector targetVector = new FloatVec(new float[]{0.145292f, 0.914725f, 0.796505f, 0.700925f, 0.560520f});
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(targetVector))
                .annsField(DENSE_FIELD)
                .limit(limit)
                .outputFields(Arrays.asList(DOCID_FIELD, TEXT_FIELD))
                .groupByFieldName(groupField)
                .groupSize(groupSize)
                .strictGroupSize(strictGroup)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        System.out.printf("Search with group by field: %s, group size: %d, strict: %b, limit: %d%n",
                groupField, groupSize, strictGroup, limit);
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }

    private static void hybridSearchGroupBy(MilvusClientV2 client, String groupField, long limit, int groupSize, boolean strictGroup) {
        System.out.println("\n====================================================================================");
        BaseVector targetVector = new FloatVec(new float[]{0.145292f, 0.914725f, 0.796505f, 0.700925f, 0.560520f});
        BaseVector targetText = new EmbeddedText("Milvus vector database");
        List<AnnSearchReq> searchRequests = new ArrayList<>();
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName(DENSE_FIELD)
                .vectors(Collections.singletonList(targetVector))
                .limit(limit * 2)
                .build());
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName(SPARSE_FIELD)
                .vectors(Collections.singletonList(targetText))
                .limit(limit * 2)
                .build());

        SearchResp searchResp = client.hybridSearch(HybridSearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .searchRequests(searchRequests)
                .functionScore(FunctionScore.builder()
                        .addFunction(WeightedRanker.builder().weights(Arrays.asList(0.5f, 0.5f)).build())
                        .build())
                .limit(limit)
                .outFields(Arrays.asList(DOCID_FIELD, TEXT_FIELD))
                .groupByFieldName(groupField)
                .groupSize(groupSize)
                .strictGroupSize(strictGroup)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        System.out.printf("HybridSearch with group by field: %s, group size: %d, strict: %b, limit: %d%n",
                groupField, groupSize, strictGroup, limit);
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
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
                .fieldName(DENSE_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(SPARSE_FIELD)
                .dataType(DataType.SparseFloatVector)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(TEXT_FIELD)
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .enableAnalyzer(true) // must enable this if you use Function
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(DOCID_FIELD)
                .dataType(DataType.Int32)
                .build());

        // With this function, milvus will convert the strings of "text" field to sparse vectors of "vector" field
        // by built-in tokenizer and analyzer
        // Read the link for more info: https://milvus.io/docs/full-text-search.md
        schema.addFunction(Function.builder()
                .functionType(FunctionType.BM25)
                .name("function_bm25")
                .inputFieldNames(Collections.singletonList(TEXT_FIELD))
                .outputFieldNames(Collections.singletonList(SPARSE_FIELD))
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(DENSE_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName(SPARSE_FIELD)
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
                gson.fromJson("{\"id\": 0, \"text\": \"Milvus is an open-source vector database\", \"dense\": [0.358037, -0.602349, 0.184140, -0.262862, 0.902943], \"docId\": 1}", JsonObject.class),
                gson.fromJson("{\"id\": 1, \"text\": \"AI applications help people better life\", \"dense\": [0.198868, 0.060235, 0.697696, 0.261447, 0.838729], \"docId\": 5}", JsonObject.class),
                gson.fromJson("{\"id\": 2, \"text\": \"Will the electric car replace gas-powered car?\", \"dense\": [0.437421, -0.559750, 0.645788, 0.789405, 0.207857], \"docId\": 2}", JsonObject.class),
                gson.fromJson("{\"id\": 3, \"text\": \"LangChain is a composable framework to build with LLMs. Milvus is integrated into LangChain.\", \"dense\": [0.317200, 0.971904, -0.369811, 0.120690, -0.144627], \"docId\": 4}", JsonObject.class),
                gson.fromJson("{\"id\": 4, \"text\": \"RAG is the process of optimizing the output of a large language model\", \"dense\": [0.837197, -0.015764, -0.310629, -0.562666, -0.898494], \"docId\": 1}", JsonObject.class),
                gson.fromJson("{\"id\": 5, \"text\": \"Newton is one of the greatest scientist of human history\", \"dense\": [-0.33445, -0.256713, 0.898753, 0.940299, 0.537806], \"docId\": 2}", JsonObject.class),
                gson.fromJson("{\"id\": 6, \"text\": \"Metric type L2 is Euclidean distance\", \"dense\": [0.395247, 0.400025, -0.589050, -0.865050, -0.6140360], \"docId\": 5}", JsonObject.class),
                gson.fromJson("{\"id\": 7, \"text\": \"Embeddings represent real-world objects, like words, images, or videos, in a form that computers can process.\", \"dense\": [0.571828, 0.240703, -0.373791, -0.067269, -0.6980531], \"docId\": 3}", JsonObject.class)
        );

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows in collection\n", (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // search without group by, limit 3
        searchGroupBy(client, null, 3, 0, false);
        // search group by docId, limit 3, group size 1, strict false
        searchGroupBy(client, DOCID_FIELD, 3, 1, false);
        // search group by docId, limit 3, group size 3, strict false
        searchGroupBy(client, DOCID_FIELD, 3, 2, false);
        // search group by docId, limit 3, group size 3, strict true
        searchGroupBy(client, DOCID_FIELD, 3, 2, true);
        // search group by docId, limit 4, group size 3, strict false
        searchGroupBy(client, DOCID_FIELD, 4, 3, false);
        // search group by docId, limit 4, group size 3, strict false
        searchGroupBy(client, DOCID_FIELD, 4, 3, true);

        // hybrid search without group by, limit 3
        hybridSearchGroupBy(client, null, 3, 0, false);
        // hybrid search group by docId, limit 3, group size 2, strict false
        hybridSearchGroupBy(client, DOCID_FIELD, 3, 2, false);
        // hybrid search group by docId, limit 3, group size 2, strict true
        hybridSearchGroupBy(client, DOCID_FIELD, 3, 2, true);

        client.close();
    }
}

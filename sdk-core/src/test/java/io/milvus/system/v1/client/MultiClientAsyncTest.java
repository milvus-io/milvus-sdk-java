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

package io.milvus.system.v1.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.milvus.support.v1.MilvusMultiDockerTestBase;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

@Tag("system")
class MultiClientAsyncTest extends MilvusMultiDockerTestBase {

    @Test
    void testAsyncMethods() {
        String randomCollectionName = generator.generate(10);

        // collection schema
        String field1Name = "long_field";
        String field2Name = "vec_field";
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(true)
                .withDataType(DataType.Int64)
                .withName(field1Name)
                .withDescription("identity")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(DataType.FloatVector)
                .withName(field2Name)
                .withDescription("face")
                .withDimension(DIMENSION)
                .build());

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withDescription("test")
                .withFieldTypes(fieldsSchema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert async
        List<ListenableFuture<R<MutationResult>>> futureResponses = new ArrayList<>();
        int rowCount = 1000;
        for (long i = 0L; i < 10; ++i) {
            List<List<Float>> vectors = utils.generateFloatVectors(rowCount);
            List<InsertParam.Field> fieldsInsert = new ArrayList<>();
            fieldsInsert.add(new InsertParam.Field(field2Name, vectors));

            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(randomCollectionName)
                    .withFields(fieldsInsert)
                    .build();

            ListenableFuture<R<MutationResult>> insertFuture = client.insertAsync(insertParam);
            futureResponses.add(insertFuture);
        }

        // get insert result
        List<Long> queryIDs = new ArrayList<>();
        for (ListenableFuture<R<MutationResult>> response : futureResponses) {
            try {
                R<MutationResult> insertR = response.get();
                assertEquals(R.Status.Success.getCode(), insertR.getStatus().intValue());

                MutationResultWrapper wrapper = new MutationResultWrapper(insertR.getData());
                queryIDs.add(wrapper.getLongIDs().get(0));
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("failed to insert:" + e.getMessage());
                return;
            }
        }

        // get collection statistics
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(field2Name)
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.IP)
                .withSyncMode(Boolean.TRUE)
                .withSyncWaitingInterval(500L)
                .withSyncWaitingTimeout(30L)
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // search async
        List<List<Float>> targetVectors = utils.generateFloatVectors(2);
        int topK = 5;
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withMetricType(MetricType.IP)
                .withTopK(topK)
                .withVectors(targetVectors)
                .withVectorFieldName(field2Name)
                .build();

        ListenableFuture<R<SearchResults>> searchFuture = client.searchAsync(searchParam);

        // query async
        String expr = field1Name + " in " + queryIDs;
        List<String> outputFields = Arrays.asList(field1Name, field2Name);
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr(expr)
                .withOutFields(outputFields)
                .build();

        ListenableFuture<R<QueryResults>> queryFuture = client.queryAsync(queryParam);

        try {
            // get search results
            R<SearchResults> searchR = searchFuture.get();
            assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());

            // verify search result
            SearchResultsWrapper results = new SearchResultsWrapper(searchR.getData().getResults());
//            System.out.println("Search results:");
            for (int i = 0; i < targetVectors.size(); ++i) {
                List<SearchResultsWrapper.IDScore> scores = results.getIDScore(i);
                assertEquals(topK, scores.size());
//                System.out.println(scores);
            }

            // get query results
            R<QueryResults> queryR = queryFuture.get();
            assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

            // verify query result
            QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
            for (String fieldName : outputFields) {
                FieldDataWrapper wrapper = queryResultsWrapper.getFieldWrapper(fieldName);
                System.out.println("Query data of " + fieldName + ", row count: " + wrapper.getRowCount());
//                System.out.println(wrapper.getFieldData());
                assertEquals(queryIDs.size(), wrapper.getFieldData().size());
            }

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // drop collection
        DropCollectionParam dropParam = DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build();

        R<RpcStatus> dropR = client.dropCollection(dropParam);
        assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }
}

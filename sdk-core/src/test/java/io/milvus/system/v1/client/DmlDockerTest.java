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

import com.google.gson.JsonObject;
import io.milvus.support.v1.MilvusV1DockerTestBase;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.common.utils.JsonUtils;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.*;
import io.milvus.param.dml.ranker.WeightedRanker;
import io.milvus.param.index.*;
import io.milvus.response.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Tag("system")
class DmlDockerTest extends MilvusV1DockerTestBase {

    @Test
    void testUpsert() throws InterruptedException {
        String randomCollectionName = generator.generate(10);

        // collection schema
        CollectionSchemaParam schema = buildSchema(false, false, true,
                Arrays.asList(DataType.FloatVector, DataType.VarChar));

        // create collection
        CreateCollectionParam createParam = CreateCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withSchema(schema)
                .build();

        R<RpcStatus> createR = client.createCollection(createParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createR.getStatus().intValue());

        // insert data by row-based with id from 0 ~ 9
        int rowCount = 10;
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
            row.addProperty(DataType.VarChar.name(), String.format("name_%d", i));
            row.addProperty("dynamic_value", String.format("dynamic_%d", i));
            rows.add(row);
        }

        InsertParam insertRowParam = InsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> insertRowResp = client.insert(insertRowParam);
        Assertions.assertEquals(R.Status.Success.getCode(), insertRowResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // get collection statistics with flush, the 10 rows are flushed to a sealed segment
        // wait 2 seconds, ensure the data node consumes the data
        TimeUnit.SECONDS.sleep(2);
        R<GetCollectionStatisticsResponse> statR = client.getCollectionStatistics(GetCollectionStatisticsParam
                .newBuilder()
                .withCollectionName(randomCollectionName)
                .withFlush(true)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), statR.getStatus().intValue());

        GetCollStatResponseWrapper stat = new GetCollStatResponseWrapper(statR.getData());
        System.out.println("Collection row count: " + stat.getRowCount());

        // create index
        CreateIndexParam indexParam = CreateIndexParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withFieldName(DataType.FloatVector.name())
                .withIndexName("abv")
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .withExtraParam("{}")
                .build();

        R<RpcStatus> createIndexR = client.createIndex(indexParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createIndexR.getStatus().intValue());

        // load collection
        R<RpcStatus> loadR = client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), loadR.getStatus().intValue());

        // retrieve one row from the sealed segment
        QueryParam queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id == 5")
                .addOutField(DataType.VarChar.name())
                .build();

        R<QueryResults> queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        QueryResultsWrapper queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        List<QueryResultsWrapper.RowRecord> records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results in sealed segment:");
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
            Object name = record.get(DataType.VarChar.name());
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_5", name);
        }

        // insert 10 rows into growing segment with id from 10 ~ 19
        // since the ids are not exist, the upsert call is equal to an insert call
        rows.clear();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty("id", rowCount + i);
            List<Float> vector = utils.generateFloatVectors(1).get(0);
            row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
            row.addProperty(DataType.VarChar.name(), String.format("name_%d", rowCount + i));
            rows.add(row);
        }

        UpsertParam upsertParam = UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        R<MutationResult> upsertResp = client.upsert(upsertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), upsertResp.getStatus().intValue());
        System.out.println(rowCount + " rows inserted");

        // retrieve one row from the growing segment
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id == 18")
                .addOutField(DataType.VarChar.name())
                .addOutField("dynamic_value")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        System.out.println("Query results in growing segment:");
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
            Object name = record.get(DataType.VarChar.name());
            Assertions.assertNotNull(name);
            Assertions.assertEquals("name_18", name);
            Assertions.assertFalse(record.contains("dynamic_value"));
            Assertions.assertNull(record.get("dynamic_value")); // we didn't set dynamic_value for No.18 row
        }

        // upsert to change the no.5 and no.18 items
        rows.clear();
        JsonObject row = new JsonObject();
        row.addProperty("id", 5L);
        List<Float> vector = utils.generateFloatVectors(1).get(0);
        row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
        row.addProperty(DataType.VarChar.name(), "updated_5");
        row.addProperty("dynamic_value", String.format("dynamic_%d", 5));
        rows.add(row);
        row = new JsonObject();
        row.addProperty("id", 18L);
        vector = utils.generateFloatVectors(1).get(0);
        row.add(DataType.FloatVector.name(), JsonUtils.toJsonTree(vector));
        row.addProperty(DataType.VarChar.name(), "updated_18");
        row.addProperty("dynamic_value", 18);
        rows.add(row);

        upsertParam = UpsertParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withRows(rows)
                .build();

        upsertResp = client.upsert(upsertParam);
        Assertions.assertEquals(R.Status.Success.getCode(), upsertResp.getStatus().intValue());

        // verify the two items
        queryParam = QueryParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .withExpr("id == 5 || id == 18")
                .addOutField(DataType.VarChar.name())
                .addOutField("dynamic_value")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build();

        queryR = client.query(queryParam);
        Assertions.assertEquals(R.Status.Success.getCode(), queryR.getStatus().intValue());

        queryResultsWrapper = new QueryResultsWrapper(queryR.getData());
        records = queryResultsWrapper.getRowRecords();
        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals("updated_5", records.get(0).get(DataType.VarChar.name()));
        Assertions.assertEquals("dynamic_5", records.get(0).get("dynamic_value"));
        Assertions.assertEquals("updated_18", records.get(1).get(DataType.VarChar.name()));
        Assertions.assertEquals(18L, records.get(1).get("dynamic_value"));

        // drop collection
        R<RpcStatus> dropR = client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(randomCollectionName)
                .build());
        Assertions.assertEquals(R.Status.Success.getCode(), dropR.getStatus().intValue());
    }

    @Test
    void testConsistencyLevel() {
        String randomCollectionName = generator.generate(10);
        String pkName = "pk";
        String vectorName = "vector";
        int dim = 4;
        String defaultDbName = "default";
        String tempDbName = "db_for_level";

        // create a temp database
        CreateDatabaseParam createDatabaseParam = CreateDatabaseParam.newBuilder()
                .withDatabaseName(tempDbName)
                .build();
        R<RpcStatus> createResponse = client.createDatabase(createDatabaseParam);
        Assertions.assertEquals(R.Status.Success.getCode(), createResponse.getStatus().intValue());

        Function<String, Void> runTestFunc =
                dbName -> {
                    // a client use the temp database
                    ConnectParam connectParam = connectParamBuilder()
                            .withDatabaseName(tempDbName)
                            .build();
                    MilvusClientForTest tempClient = new MilvusClientForTest(connectParam);

                    for (int i = 0; i < 20; i++) {
                        JsonObject row = new JsonObject();
                        row.addProperty(pkName, i);
                        row.add(vectorName, JsonUtils.toJsonTree(utils.generateFloatVector(dim)));
                        tempClient.insert(InsertParam.newBuilder()
                                .withDatabaseName(dbName)
                                .withCollectionName(randomCollectionName)
                                .withRows(Collections.singletonList(row))
                                .build());

                        // query/search/hybridSearch immediately after insert, data must be visible
                        String expr = String.format("%s == %d", pkName, i);
                        if (i % 3 == 0) {
                            R<QueryResults> fetchR = tempClient.query(QueryParam.newBuilder()
                                    .withDatabaseName(dbName)
                                    .withCollectionName(randomCollectionName)
                                    .withExpr(expr)
                                    .withLimit(5L)
                                    .addOutField(pkName)
                                    .build());
                            Assertions.assertEquals(R.Status.Success.getCode(), fetchR.getStatus().intValue());
                            QueryResultsWrapper oneResult = new QueryResultsWrapper(fetchR.getData());
                            List<QueryResultsWrapper.RowRecord> records = oneResult.getRowRecords();
                            Assertions.assertEquals(1L, records.size());
                        } else if (i % 2 == 0) {
                            R<SearchResults> searchOne = tempClient.search(SearchParam.newBuilder()
                                    .withDatabaseName(dbName)
                                    .withCollectionName(randomCollectionName)
                                    .withVectorFieldName(vectorName)
                                    .withLimit(5L)
                                    .withExpr(expr)
                                    .withFloatVectors(Collections.singletonList(utils.generateFloatVector(dim)))
                                    .addOutField(pkName)
                                    .build());
                            Assertions.assertEquals(R.Status.Success.getCode(), searchOne.getStatus().intValue());

                            SearchResultsWrapper oneResult = new SearchResultsWrapper(searchOne.getData().getResults());
                            List<SearchResultsWrapper.IDScore> scores = oneResult.getIDScore(0);
                            Assertions.assertEquals(1, scores.size());
                        } else {
                            AnnSearchParam subReq = AnnSearchParam.newBuilder()
                                    .withVectorFieldName(vectorName)
                                    .withExpr(expr)
                                    .withFloatVectors(Collections.singletonList(utils.generateFloatVector(dim)))
                                    .withLimit(5L)
                                    .build();

                            R<SearchResults> searchR = tempClient.hybridSearch(HybridSearchParam.newBuilder()
                                    .withDatabaseName(dbName)
                                    .withCollectionName(randomCollectionName)
                                    .addSearchRequest(subReq)
                                    .withLimit(5L)
                                    .withRanker(WeightedRanker.newBuilder()
                                            .withWeights(Collections.singletonList(1.0f))
                                            .build())
                                    .withOutFields(Collections.singletonList(pkName))
                                    .build());
                            Assertions.assertEquals(R.Status.Success.getCode(), searchR.getStatus().intValue());
                            SearchResultsWrapper oneResult = new SearchResultsWrapper(searchR.getData().getResults());
                            List<SearchResultsWrapper.IDScore> scores = oneResult.getIDScore(0);
                            Assertions.assertEquals(1, scores.size());
                        }
                    }
                    return null;
                };

        // test SESSION level
        createSimpleCollection(client, "", randomCollectionName, pkName, false, dim, ConsistencyLevelEnum.SESSION);
        runTestFunc.apply(defaultDbName);

        createSimpleCollection(client, tempDbName, randomCollectionName, pkName, false, dim, ConsistencyLevelEnum.SESSION);
        runTestFunc.apply(tempDbName);

        // test STRONG level
        createSimpleCollection(client, "", randomCollectionName, pkName, false, dim, ConsistencyLevelEnum.STRONG);
        runTestFunc.apply(defaultDbName);

        createSimpleCollection(client, tempDbName, randomCollectionName, pkName, false, dim, ConsistencyLevelEnum.STRONG);
        runTestFunc.apply(tempDbName);
    }
}

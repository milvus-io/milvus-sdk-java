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

package io.milvus.system.v2.client;

import io.milvus.support.v2.MilvusV2DockerTestBase;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.support.TestUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.response.*;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class ConsistencyDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testConsistencyLevel() throws InterruptedException {
        String randomCollectionName = generator.generate(10);
        String pkName = "pk";
        String vectorName = "vector";
        int dim = 4;
        String defaultDbName = "default";
        String tempDbName = "test_level_db";

        // create a temp database
        client.createDatabase(CreateDatabaseReq.builder()
                .databaseName(tempDbName)
                .build());

        Function<String, Void> runTestFunc =
                dbName -> {
                    // a client use the temp database
                    ConnectConfig config = ConnectConfig.builder()
                            .uri(TestUtils.MilvusStandaloneUri)
                            .dbName(tempDbName)
                            .build();
                    MilvusClientV2 tempClient = null;
                    try {
                        tempClient = new MilvusClientV2(config);

                        for (int i = 0; i < 20; i++) {
                            JsonObject row = new JsonObject();
                            row.addProperty(pkName, i);
                            row.add(vectorName, JsonUtils.toJsonTree(utils.generateFloatVector(dim)));
                            tempClient.insert(InsertReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                    .data(Collections.singletonList(row)).build());

                            // query/search/hybridSearch immediately after insert, data must be visible
                            String filter = String.format("%s == %d", pkName, i);
                            if (i % 3 == 0) {
                                QueryResp queryResp = client.query(
                                        QueryReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                                .filter(filter).outputFields(Collections.singletonList(pkName)).build());
                                List<QueryResp.QueryResult> oneResult = queryResp.getQueryResults();
                                Assertions.assertEquals(1, oneResult.size());
                            } else if (i % 2 == 0) {
                                SearchResp searchResp = client.search(
                                        SearchReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                                .annsField(vectorName).filter(filter)
                                                .data(Collections.singletonList(new FloatVec(utils.generateFloatVector(dim))))
                                                .limit(10).build());
                                List<List<SearchResp.SearchResult>> oneResult = searchResp.getSearchResults();
                                Assertions.assertEquals(1, oneResult.size());
                                Assertions.assertEquals(1, oneResult.get(0).size());
                            } else {
                                AnnSearchReq subReq = AnnSearchReq.builder().vectorFieldName(vectorName).filter(filter)
                                        .vectors(Collections.singletonList(new FloatVec(utils.generateFloatVector(dim))))
                                        .limit(7).build();

                                SearchResp searchResp = client.hybridSearch(
                                        HybridSearchReq.builder().databaseName(dbName).collectionName(randomCollectionName)
                                                .searchRequests(Collections.singletonList(subReq))
                                                .ranker(RRFRanker.builder().k(20).build()).limit(5).build());
                                List<List<SearchResp.SearchResult>> oneResult = searchResp.getSearchResults();
                                Assertions.assertEquals(1, oneResult.size());
                                Assertions.assertEquals(1, oneResult.get(0).size());
                            }
                        }
                    } finally {
                        if (tempClient != null) {
                            tempClient.close();
                        }
                    }
                    return null;
                };

        // test SESSION level
        createSimpleCollection(client, "", randomCollectionName, pkName, false, dim, ConsistencyLevel.SESSION);
        runTestFunc.apply(defaultDbName);

        createSimpleCollection(client, tempDbName, randomCollectionName, pkName, false, dim, ConsistencyLevel.SESSION);
        runTestFunc.apply(tempDbName);

        // test STRONG level
        createSimpleCollection(client, "", randomCollectionName, pkName, false, dim, ConsistencyLevel.STRONG);
        runTestFunc.apply(defaultDbName);

        createSimpleCollection(client, tempDbName, randomCollectionName, pkName, false, dim, ConsistencyLevel.STRONG);
        runTestFunc.apply(tempDbName);
    }

}

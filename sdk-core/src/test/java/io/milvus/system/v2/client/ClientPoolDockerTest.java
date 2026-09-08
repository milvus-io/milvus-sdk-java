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
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.support.TestUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.database.request.*;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("system")
class ClientPoolDockerTest extends MilvusV2DockerTestBase {
    @Test
    void testClientPool() {
        // create a temp database
        String dummyDb = "test_pool_db";
        client.createDatabase(CreateDatabaseReq.builder()
                .databaseName(dummyDb)
                .build());

        String collectionName = "test_pool_coll";
        client.createCollection(CreateCollectionReq.builder()
                .databaseName(dummyDb)
                .collectionName(collectionName)
                .autoID(true)
                .primaryFieldName("id")
                .vectorFieldName("vector")
                .dimension(4)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .enableDynamicField(false)
                .build());

        JsonObject row = new JsonObject();
        row.add("vector", JsonUtils.toJsonTree(utils.generateFloatVector(4)));
        client.insert(InsertReq.builder()
                .databaseName(dummyDb)
                .collectionName(collectionName)
                .data(Collections.singletonList(row))
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .databaseName(dummyDb)
                .collectionName(collectionName)
                .build());

        try {
            // the default connection config will connect to default db
            ConnectConfig connectConfig = ConnectConfig.builder()
                    .uri(TestUtils.MilvusStandaloneUri)
                    .build();
            int minIdlePerKey = 1;
            int maxIdlePerKey = 2;
            int maxTotalPerKey = 4;
            PoolConfig poolConfig = PoolConfig.builder()
                    .minIdlePerKey(minIdlePerKey)
                    .maxIdlePerKey(maxIdlePerKey)
                    .maxTotalPerKey(maxTotalPerKey)
                    .build();
            MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig);

            // clients of the key "dummy_db" will connect to this db
            pool.configForKey(dummyDb, ConnectConfig.builder()
                    .uri(TestUtils.MilvusStandaloneUri)
                    .dbName(dummyDb)
                    .rpcDeadlineMs(100L)
                    .build());
            Set<String> keys = pool.configKeys();
            Assertions.assertTrue(keys.contains(dummyDb));
            ConnectConfig dummyConfig = pool.getConfig(dummyDb);
            Assertions.assertEquals(dummyDb, dummyConfig.getDbName());

            pool.preparePool(dummyDb);
            Assertions.assertEquals(minIdlePerKey, pool.getActiveClientNumber(dummyDb));

            class Worker implements Runnable {
                private int id = 0;

                public Worker(int id) {
                    this.id = id;
                }

                @Override
                public void run() {
                    MilvusClientV2 client = null;
                    try {
                        client = pool.getClient(dummyDb);
                        Assertions.assertEquals(dummyDb, client.currentUsedDatabase());

                        FloatVec vector = new FloatVec(utils.generateFloatVector(4));
                        SearchResp resp = client.search(SearchReq.builder()
                                .collectionName(collectionName)
                                .limit(1)
                                .data(Collections.singletonList(vector))
                                .build());
                        Assertions.assertEquals(1, resp.getSearchResults().size());

                        if ((id + 1) % 10000 == 0) {
                            System.out.printf("current qps: %.2f%n", pool.fetchClientPerSecond(dummyDb));
                        }
                    } catch (Exception e) {
                        System.out.printf("request failed: %s%n", e);
                    } finally {
                        pool.returnClient(dummyDb, client);
                    }
                }
            }
            long start = System.currentTimeMillis();
            int threadCount = 20;
            int requestCount = 50000;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < requestCount; i++) {
                Runnable worker = new Worker(i);
                executor.execute(worker);
            }
            executor.shutdown();
            if (!executor.awaitTermination(100, TimeUnit.SECONDS)) {
                System.err.println("Executor did not terminate in the specified time.");
                Assertions.fail();
            }
            Assertions.assertEquals(maxTotalPerKey, pool.getActiveClientNumber(dummyDb));
            Assertions.assertEquals(maxTotalPerKey, pool.getTotalActiveClientNumber());

            long end = System.currentTimeMillis();
            System.out.printf("time cost: %dms, average qps: %f%n", end - start, (float) requestCount * 1000 / (end - start));
            System.out.printf("idle %d, active %d%n", pool.getIdleClientNumber(dummyDb), pool.getActiveClientNumber(dummyDb));
            System.out.printf("total idle %d, total active %d%n", pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());

            while (pool.getActiveClientNumber(dummyDb) > 1) {
                TimeUnit.SECONDS.sleep(1);
                System.out.printf("waiting idle %d, active %d%n", pool.getIdleClientNumber(dummyDb), pool.getActiveClientNumber(dummyDb));
            }
            Assertions.assertEquals(maxIdlePerKey, pool.getIdleClientNumber(dummyDb));
            Assertions.assertEquals(maxIdlePerKey, pool.getTotalIdleClientNumber());
            Assertions.assertEquals(1, pool.getActiveClientNumber(dummyDb));
            Assertions.assertEquals(1, pool.getTotalActiveClientNumber());

            // get client connect to the dummy db
            MilvusClientV2 dummyClient = pool.getClient(dummyDb);
            Assertions.assertEquals(dummyDb, dummyClient.currentUsedDatabase());
            pool.removeConfig(dummyDb);
            Assertions.assertNull(pool.getConfig(dummyDb));
            pool.close();

            client.dropCollection(DropCollectionReq.builder()
                    .databaseName(dummyDb)
                    .collectionName(collectionName)
                    .build());
            client.dropDatabase(DropDatabaseReq.builder()
                    .databaseName(dummyDb)
                    .build());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }
    }

}

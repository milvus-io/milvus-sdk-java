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
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClientPoolExample {
    public static String serverUri = "http://localhost:19530";
    public static String CollectionName = "java_sdk_example_pool_v2";
    public static String VectorFieldName = "vector";
    public static int DIM = 128;
    public static List<String> dbNames = Arrays.asList("AA", "BB", "CC");

    private static void printKeyClientNumber(MilvusClientV2Pool pool, String key) {
        System.out.printf("Key '%s': %d idle clients and %d active clients%n",
                key, pool.getIdleClientNumber(key), pool.getActiveClientNumber(key));
    }
    private static void printClientNumber(MilvusClientV2Pool pool) {
        System.out.println("======================================================================");
        System.out.printf("Total %d idle clients and %d active clients%n",
                pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());
        for (String dbName : dbNames) {
            printKeyClientNumber(pool, dbName);
        }
        System.out.println("======================================================================");
    }

    public static void createDatabases(MilvusClientV2Pool pool) {
        // get a client, the client uses the default config to connect milvus(to the default database)
        String tempKey = "temp";
        MilvusClientV2 client = pool.getClient(tempKey);
        if (client == null) {
            throw new RuntimeException("Unable to create client");
        }
        try {
            for (String dbName : dbNames) {
                client.createDatabase(CreateDatabaseReq.builder()
                        .databaseName(dbName)
                        .build());
                System.out.println("Database created: " + dbName);
            }
        } catch (Exception e) {
            String msg = String.format("Failed to create database, error: %s%n", e.getMessage());
            System.out.printf(msg);
            throw new RuntimeException(msg);
        } finally {
            pool.returnClient(tempKey, client);
            pool.clear(tempKey); // this call will destroy the temp client
        }

        // predefine a connect config for each key(name of a database)
        // the ClientPool will use different config to create client to connect to specific database
        for (String dbName : dbNames) {
            ConnectConfig config = ConnectConfig.builder()
                    .uri(serverUri)
                    .dbName(dbName)
                    .build();
            pool.configForKey(dbName, config);
        }
    }

    public static void createCollections(MilvusClientV2Pool pool) {
        for (String dbName : dbNames) {
            // this client connects to the database of dbName because we have predefined
            // a ConnectConfig for this key
            MilvusClientV2 client = pool.getClient(dbName);
            if (client == null) {
                throw new RuntimeException("Unable to create client");
            }
            try {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(CollectionName)
                        .build());
                client.createCollection(CreateCollectionReq.builder()
                        .collectionName(CollectionName)
                        .primaryFieldName("id")
                        .idType(DataType.Int64)
                        .autoID(Boolean.TRUE)
                        .vectorFieldName(VectorFieldName)
                        .dimension(DIM)
                        .build());
                System.out.printf("Collection '%s' created in database '%s'%n", CollectionName, dbName);
            } catch (Exception e) {
                String msg = String.format("Failed to create collection, error: %s%n", e.getMessage());
                System.out.printf(msg);
                throw new RuntimeException(msg);
            } finally {
                pool.returnClient(dbName, client);
            }
        }
    }

    public static Thread runInsertThread(MilvusClientV2Pool pool, String dbName, int repeatRequests) {
        Thread t = new Thread(() -> {
            Gson gson = new Gson();
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClientV2 client = null;
                while(client == null) {
                    try {
                        // getClient() might exceeds the borrowMaxWaitMillis and throw exception
                        // retry to call until it return a client
                        client = pool.getClient(dbName);
                    } catch (Exception e) {
                        System.out.printf("Failed to get client, will retry, error: %s%n", e.getMessage());
                    }
                }
                try {
                    int rowCount = 1;
                    List<JsonObject> rows = new ArrayList<>();
                    for (int j = 0; j < rowCount; j++) {
                        JsonObject row = new JsonObject();
                        row.add(VectorFieldName, gson.toJsonTree(CommonUtils.generateFloatVector(DIM)));
                        rows.add(row);
                    }
                    InsertResp insertR = client.insert(InsertReq.builder()
                            .collectionName(CollectionName)
                            .data(rows)
                            .build());
//                    System.out.printf("%d rows inserted%n", insertR.getInsertCnt());
                } catch (Exception e) {
                    System.out.printf("Failed to inserted, error: %s%n", e.getMessage());
                } finally {
                    pool.returnClient(dbName, client); // make sure the client is returned after use
                }
            }
            System.out.printf("Insert thread %s finished%n", Thread.currentThread().getName());
            printKeyClientNumber(pool, dbName);
        });
        t.start();
        return t;
    }

    public static Thread runSearchThread(MilvusClientV2Pool pool, String dbName, int repeatRequests) {
        Thread t = new Thread(() -> {
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClientV2 client = null;
                while(client == null) {
                    try {
                        // getClient() might exceeds the borrowMaxWaitMillis and throw exception
                        // retry to call until it return a client
                        client = pool.getClient(dbName);
                    } catch (Exception e) {
                        System.out.printf("Failed to get client, will retry, error: %s%n", e.getMessage());
                    }
                }
                try {
                    SearchResp result = client.search(SearchReq.builder()
                            .collectionName(CollectionName)
                            .consistencyLevel(ConsistencyLevel.EVENTUALLY)
                            .annsField(VectorFieldName)
                            .limit(10)
                            .data(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(DIM))))
                            .build());
//                    System.out.printf("A search request returns %d items with nq %d%n",
//                            result.getSearchResults().get(0).size(), result.getSearchResults().size());
                } catch (Exception e) {
                    System.out.printf("Failed to search, error: %s%n", e.getMessage());
                } finally {
                    pool.returnClient(dbName, client); // make sure the client is returned after use
                }
            }
            System.out.printf("Search thread %s finished%n", Thread.currentThread().getName());
            printKeyClientNumber(pool, dbName);
        });
        t.start();
        return t;
    }

    public static void verifyRowCount(MilvusClientV2Pool pool, long expectedCount) {
        for (String dbName : dbNames) {
            // this client connects to the database of dbName because we have predefined
            // a ConnectConfig for this key
            MilvusClientV2 client = pool.getClient(dbName);
            if (client == null) {
                throw new RuntimeException("Unable to create client");
            }
            try {
                QueryResp countR = client.query(QueryReq.builder()
                        .collectionName(CollectionName)
                        .outputFields(Collections.singletonList("count(*)"))
                        .consistencyLevel(ConsistencyLevel.STRONG)
                        .build());
                long rowCount = (long)countR.getQueryResults().get(0).getEntity().get("count(*)");
                System.out.printf("%d rows persisted in collection '%s' of database '%s'%n",
                        rowCount, CollectionName, dbName);
                if (rowCount != expectedCount) {
                    throw new RuntimeException("The persisted row count is not equal to expected");
                }
            } catch (Exception e) {
                String msg = String.format("Failed to get row count, error: %s%n", e.getMessage());
                System.out.printf(msg);
                throw new RuntimeException(msg);
            } finally {
                pool.returnClient(dbName, client);
            }
        }
    }

    public static void dropCollections(MilvusClientV2Pool pool) {
        for (String dbName : dbNames) {
            // this client connects to the database of dbName because we have predefined
            // a ConnectConfig for this key
            MilvusClientV2 client = pool.getClient(dbName);
            if (client == null) {
                throw new RuntimeException("Unable to create client");
            }
            try {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(CollectionName)
                        .build());
                System.out.printf("Collection '%s' dropped in database '%s'%n", CollectionName, dbName);
            } catch (Exception e) {
                String msg = String.format("Failed to drop collection, error: %s%n", e.getMessage());
                System.out.printf(msg);
                throw new RuntimeException(msg);
            } finally {
                pool.returnClient(dbName, client);
            }
        }
    }

    public static void dropDatabases(MilvusClientV2Pool pool) {
        // get a client, the client uses the default config to connect milvus(to the default database)
        String tempKey = "temp";
        MilvusClientV2 client = pool.getClient(tempKey);
        if (client == null) {
            throw new RuntimeException("Unable to create client");
        }
        try {
            for (String dbName : dbNames) {
                client.dropDatabase(DropDatabaseReq.builder()
                        .databaseName(dbName)
                        .build());
                System.out.println("Database dropped: " + dbName);
            }
        } catch (Exception e) {
            String msg = String.format("Failed to drop database, error: %s%n", e.getMessage());
            System.out.printf(msg);
            throw new RuntimeException(msg);
        } finally {
            pool.returnClient(tempKey, client);
            pool.clear(tempKey); // this call will destroy the temp client
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig defaultConfig = ConnectConfig.builder()
                .uri(serverUri)
                .build();
        // read this issue for more details about the pool configurations:
        // https://github.com/milvus-io/milvus-sdk-java/issues/1577
        PoolConfig poolConfig = PoolConfig.builder()
                .maxIdlePerKey(10) // max idle clients per key
                .maxTotalPerKey(50) // max total(idle + active) clients per key
                .maxTotal(1000) // max total clients for all keys
                .maxBlockWaitDuration(Duration.ofSeconds(5L)) // getClient() will wait 5 seconds if no idle client available
                .minEvictableIdleDuration(Duration.ofSeconds(10L)) // if number of idle clients is larger than maxIdlePerKey, redundant idle clients will be evicted after 10 seconds
                .build();
        MilvusClientV2Pool pool;
        try {
            pool = new MilvusClientV2Pool(poolConfig, defaultConfig);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return;
        }

        // create some databases
        createDatabases(pool);
        // create a collection in each database
        createCollections(pool);

        List<Thread> threadList = new ArrayList<>();
        int threadCount = 100;
        int repeatRequests = 100;
        long start = System.currentTimeMillis();
        // for each database, we create threadCount of threads to call insert() for repeatRequests times
        // each insert request will insert one row
        // for each database, we create threadCount of threads to call search() for repeatRequests times
        for (int k = 0; k < threadCount; k++) {
            for (String dbName : dbNames) {
                threadList.add(runInsertThread(pool, dbName, repeatRequests));
                threadList.add(runSearchThread(pool, dbName, repeatRequests));
            }
            printClientNumber(pool);
        }
        for (Thread t : threadList) {
            t.join();
        }
        printClientNumber(pool);

        // check row count of each collection, there are threadCount*repeatRequests rows were inserted by multiple threads
        verifyRowCount(pool, threadCount*repeatRequests);
        // drop collections
        dropCollections(pool);
        // drop databases, only after database is empty, it is able to be dropped
        dropDatabases(pool);

        long end = System.currentTimeMillis();
        System.out.printf("%d insert requests and %d search requests finished in %.3f seconds%n",
                threadCount*repeatRequests*3, threadCount*repeatRequests*3, (end-start)*0.001);

        printClientNumber(pool);
        pool.clear(); // clear idle clients
        printClientNumber(pool);
        pool.close();
    }
}

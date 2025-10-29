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
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.QueryResults;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.pool.MilvusClientV1Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.response.QueryResultsWrapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClientPoolExample {
    public static String serverUri = "http://localhost:19530";
    public static String CollectionName = "java_sdk_example_pool_v1";
    public static String VectorFieldName = "vector";
    public static int DIM = 128;
    public static List<String> dbNames = Arrays.asList("AA", "BB", "CC");

    private static void printKeyClientNumber(MilvusClientV1Pool pool, String key) {
        System.out.printf("Key '%s': %d idle clients and %d active clients%n",
                key, pool.getIdleClientNumber(key), pool.getActiveClientNumber(key));
    }

    private static void printClientNumber(MilvusClientV1Pool pool) {
        System.out.println("======================================================================");
        System.out.printf("Total %d idle clients and %d active clients%n",
                pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());
        for (String dbName : dbNames) {
            printKeyClientNumber(pool, dbName);
        }
        System.out.println("======================================================================");
    }

    public static void createDatabases(MilvusClientV1Pool pool) {
        // get a client, the client uses the default config to connect milvus(to the default database)
        String tempKey = "temp";
        MilvusClient client = pool.getClient(tempKey);
        if (client == null) {
            throw new RuntimeException("Unable to create client");
        }
        try {
            for (String dbName : dbNames) {
                client.createDatabase(CreateDatabaseParam.newBuilder()
                        .withDatabaseName(dbName)
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
            ConnectParam connectConfig = ConnectParam.newBuilder()
                    .withUri(serverUri)
                    .withDatabaseName(dbName)
                    .build();
            pool.configForKey(dbName, connectConfig);
        }
    }

    public static void createCollections(MilvusClientV1Pool pool) {
        for (String dbName : dbNames) {
            // this client connects to the database of dbName because we have predefined
            // a ConnectConfig for this key
            MilvusClient client = pool.getClient(dbName);
            if (client == null) {
                throw new RuntimeException("Unable to create client");
            }
            try {
                client.dropCollection(DropCollectionParam.newBuilder()
                        .withCollectionName(CollectionName)
                        .build());
                List<FieldType> fieldsSchema = Arrays.asList(
                        FieldType.newBuilder()
                                .withName("id")
                                .withDataType(DataType.Int64)
                                .withPrimaryKey(true)
                                .withAutoID(true)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VectorFieldName)
                                .withDataType(DataType.FloatVector)
                                .withDimension(DIM)
                                .build()
                );
                R<RpcStatus> ret = client.createCollection(CreateCollectionParam.newBuilder()
                        .withCollectionName(CollectionName)
                        .withFieldTypes(fieldsSchema)
                        .build());
                if (ret.getStatus() != R.Status.Success.getCode()) {
                    throw new RuntimeException("Failed to create collection, error: " + ret.getMessage());
                }

                ret = client.createIndex(CreateIndexParam.newBuilder()
                        .withCollectionName(CollectionName)
                        .withFieldName(VectorFieldName)
                        .withIndexType(IndexType.FLAT)
                        .withMetricType(MetricType.L2)
                        .build());
                if (ret.getStatus() != R.Status.Success.getCode()) {
                    throw new RuntimeException("Failed to create index on vector field, error: " + ret.getMessage());
                }

                client.loadCollection(LoadCollectionParam.newBuilder()
                        .withCollectionName(CollectionName)
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

    public static Thread runInsertThread(MilvusClientV1Pool pool, String dbName, int repeatRequests) {
        Thread t = new Thread(() -> {
            Gson gson = new Gson();
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClient client = null;
                while (client == null) {
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

                    R<MutationResult> insertRet = client.insert(InsertParam.newBuilder()
                            .withCollectionName(CollectionName)
                            .withRows(rows)
                            .build());
                    if (insertRet.getStatus() != R.Status.Success.getCode()) {
                        throw new RuntimeException("Failed to insert, error: " + insertRet.getMessage());
                    }
//                    System.out.printf("%d rows inserted%n", rows.size());
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

    public static Thread runSearchThread(MilvusClientV1Pool pool, String dbName, int repeatRequests) {
        Thread t = new Thread(() -> {
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClient client = null;
                while (client == null) {
                    try {
                        // getClient() might exceeds the borrowMaxWaitMillis and throw exception
                        // retry to call until it return a client
                        client = pool.getClient(dbName);
                    } catch (Exception e) {
                        System.out.printf("Failed to get client, will retry, error: %s%n", e.getMessage());
                    }
                }
                try {
                    R<SearchResults> searchRet = client.search(SearchParam.newBuilder()
                            .withCollectionName(CollectionName)
                            .withMetricType(MetricType.L2)
                            .withLimit(10L)
                            .withFloatVectors(Collections.singletonList(CommonUtils.generateFloatVector(DIM)))
                            .withVectorFieldName(VectorFieldName)
                            .withParams("{}")
                            .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                            .build());
                    if (searchRet.getStatus() != R.Status.Success.getCode()) {
                        throw new RuntimeException("Failed to search, error: " + searchRet.getMessage());
                    }
//                    System.out.println("A search request completed");
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

    public static void verifyRowCount(MilvusClientV1Pool pool, long expectedCount) {
        for (String dbName : dbNames) {
            // this client connects to the database of dbName because we have predefined
            // a ConnectConfig for this key
            MilvusClient client = pool.getClient(dbName);
            if (client == null) {
                throw new RuntimeException("Unable to create client");
            }
            try {
                R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                        .withCollectionName(CollectionName)
                        .withExpr("")
                        .addOutField("count(*)")
                        .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                        .build());
                QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
                long rowCount = (long) queryWrapper.getFieldWrapper("count(*)").getFieldData().get(0);
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

    public static void dropCollections(MilvusClientV1Pool pool) {
        for (String dbName : dbNames) {
            // this client connects to the database of dbName because we have predefined
            // a ConnectConfig for this key
            MilvusClient client = pool.getClient(dbName);
            if (client == null) {
                throw new RuntimeException("Unable to create client");
            }
            try {
                client.dropCollection(DropCollectionParam.newBuilder()
                        .withCollectionName(CollectionName)
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

    public static void dropDatabases(MilvusClientV1Pool pool) {
        // get a client, the client uses the default config to connect milvus(to the default database)
        String tempKey = "temp";
        MilvusClient client = pool.getClient(tempKey);
        if (client == null) {
            throw new RuntimeException("Unable to create client");
        }
        try {
            for (String dbName : dbNames) {
                client.dropDatabase(DropDatabaseParam.newBuilder()
                        .withDatabaseName(dbName)
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
        ConnectParam connectConfig = ConnectParam.newBuilder()
                .withUri(serverUri)
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
        MilvusClientV1Pool pool;
        try {
            pool = new MilvusClientV1Pool(poolConfig, connectConfig);
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
        verifyRowCount(pool, threadCount * repeatRequests);
        // drop collections
        dropCollections(pool);
        // drop databases, only after database is empty, it is able to be dropped
        dropDatabases(pool);

        long end = System.currentTimeMillis();
        System.out.printf("%d insert requests and %d search requests finished in %.3f seconds%n",
                threadCount * repeatRequests * 3, threadCount * repeatRequests * 3, (end - start) * 0.001);

        printClientNumber(pool);
        pool.clear(); // clear idle clients
        printClientNumber(pool);
        pool.close();
    }
}

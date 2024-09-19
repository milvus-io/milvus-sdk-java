package io.milvus.v1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SearchResults;
import io.milvus.param.*;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.pool.MilvusClientV1Pool;
import io.milvus.pool.PoolConfig;

import java.time.Duration;
import java.util.*;

public class ClientPoolExample {
    public static String CollectionName = "java_sdk_example_pool_v2";
    public static String VectorFieldName = "vector";
    public static int DIM = 128;

    public static void createCollection(MilvusClientV1Pool pool) {
        String tempKey = "temp";
        MilvusClient client = pool.getClient(tempKey);
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

            // Create the collection with 3 fields
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

            System.out.printf("Collection '%s' created%n", CollectionName);
        } catch (Exception e) {
            String msg = String.format("Failed to create collection, error: %s%n", e.getMessage());
            System.out.printf(msg);
            throw new RuntimeException(msg);
        } finally {
            pool.returnClient(tempKey, client);
            pool.clear(tempKey);
        }
    }

    public static Thread runInsertThread(MilvusClientV1Pool pool, String clientName, int repeatRequests) {
        Thread t = new Thread(() -> {
            Gson gson = new Gson();
            Random rand = new Random();
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClient client = pool.getClient(clientName);
                try {
                    int rowCount = rand.nextInt(10) + 10;
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
                    System.out.printf("%d rows inserted%n", rows.size());
                } catch (Exception e) {
                    System.out.printf("Failed to inserted, error: %s%n", e.getMessage());
                } finally {
                    pool.returnClient(clientName, client); // make sure the client is returned after use
                }
            }
            System.out.printf("Insert thread %s finished%n", Thread.currentThread().getName());
        });
        t.start();
        return t;
    }

    public static Thread runSearchThread(MilvusClientV1Pool pool, String clientName, int repeatRequests) {
        Thread t = new Thread(() -> {
            for (int i = 0; i < repeatRequests; i++) {
                MilvusClient client = pool.getClient(clientName);
                try {
                    R<SearchResults> searchRet = client.search(SearchParam.newBuilder()
                            .withCollectionName(CollectionName)
                            .withMetricType(MetricType.L2)
                            .withTopK(10)
                            .withVectors(Collections.singletonList(CommonUtils.generateFloatVector(DIM)))
                            .withVectorFieldName(VectorFieldName)
                            .withParams("{}")
                            .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                            .build());
                    if (searchRet.getStatus() != R.Status.Success.getCode()) {
                        throw new RuntimeException("Failed to search, error: " + searchRet.getMessage());
                    }
                    System.out.println("A search request completed");
                } catch (Exception e) {
                    System.out.printf("Failed to search, error: %s%n", e.getMessage());
                } finally {
                    pool.returnClient(clientName, client); // make sure the client is returned after use
                }
            }
            System.out.printf("Search thread %s finished%n", Thread.currentThread().getName());
        });
        t.start();
        return t;
    }

    public static void main(String[] args) throws InterruptedException {
        ConnectParam connectConfig = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();
        PoolConfig poolConfig = PoolConfig.builder()
                .maxIdlePerKey(10) // max idle clients per key
                .maxTotalPerKey(20) // max total(idle + active) clients per key
                .maxTotal(100) // max total clients for all keys
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

        createCollection(pool);

        List<Thread> threadList = new ArrayList<>();
        int threadCount = 10;
        int repeatRequests = 100;
        long start = System.currentTimeMillis();
        for (int k = 0; k < threadCount; k++) {
            threadList.add(runInsertThread(pool, "192.168.1.1", repeatRequests));
            threadList.add(runInsertThread(pool, "192.168.1.2", repeatRequests));
            threadList.add(runInsertThread(pool, "192.168.1.3", repeatRequests));

            threadList.add(runSearchThread(pool, "192.168.1.1", repeatRequests));
            threadList.add(runSearchThread(pool, "192.168.1.2", repeatRequests));
            threadList.add(runSearchThread(pool, "192.168.1.3", repeatRequests));

            System.out.printf("Total %d idle clients and %d active clients%n",
                    pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());
        }

        for (Thread t : threadList) {
            t.join();
        }
        long end = System.currentTimeMillis();
        System.out.printf("%d insert requests and %d search requests finished in %.3f seconds%n",
                threadCount*repeatRequests, threadCount*repeatRequests, (end-start)*0.001);
        System.out.printf("Total %d idle clients and %d active clients%n",
                pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());
        pool.clear(); // clear idle clients
        System.out.printf("After clear, total %d idle clients and %d active clients%n",
                pool.getTotalIdleClientNumber(), pool.getTotalActiveClientNumber());
    }
}

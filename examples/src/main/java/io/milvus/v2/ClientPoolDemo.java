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
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ClientPoolDemo {
    private static final String ServerUri = "http://localhost:19530";
    private static final String CollectionName = "java_sdk_example_pool_demo";
    private static final String IDFieldName = "id";
    private static final String VectorFieldName = "vector";
    private static final String TextFieldName = "text";
    private static final int DIM = 256;
    private static final String DemoKey = "for_demo";

    private static final MilvusClientV2Pool pool;

    static {
        ConnectConfig defaultConnectConfig = ConnectConfig.builder()
                .uri(ServerUri)
                .build();
        // read this issue for more details about the pool configurations:
        // https://github.com/milvus-io/milvus-sdk-java/issues/1577
        PoolConfig poolConfig = PoolConfig.builder()
                .minIdlePerKey(1)
                .maxIdlePerKey(2)
                .maxTotalPerKey(5)
                .maxBlockWaitDuration(Duration.ofSeconds(5L)) // getClient() will wait 5 seconds if no idle client available
                .build();
        try {
            pool = new MilvusClientV2Pool(poolConfig, defaultConnectConfig);
            System.out.printf("Pool is created with config:%n%s%n", poolConfig);

            // prepare the pool to pre-create some clients according to the minIdlePerKey.
            // it is like a warmup to reduce the first time cost to call the getClient()
            pool.preparePool(DemoKey);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createCollection(boolean recreate, long rowCount) {
        System.out.println("========== createCollection() ==========");
        MilvusClientV2 client = null;
        try {
            client = pool.getClient(DemoKey);
            if (client == null) {
                System.out.println("Cannot not get client from key:" + DemoKey);
                return;
            }

            if (recreate) {
                client.dropCollection(DropCollectionReq.builder()
                        .collectionName(CollectionName)
                        .build());
            } else if (client.hasCollection(HasCollectionReq.builder()
                    .collectionName(CollectionName)
                    .build())) {
                return;
            }

            CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                    .build();
            collectionSchema.addField(AddFieldReq.builder()
                    .fieldName("id")
                    .dataType(DataType.Int64)
                    .isPrimaryKey(true)
                    .autoID(true)
                    .build());
            collectionSchema.addField(AddFieldReq.builder()
                    .fieldName(VectorFieldName)
                    .dataType(DataType.FloatVector)
                    .dimension(DIM)
                    .build());
            collectionSchema.addField(AddFieldReq.builder()
                    .fieldName(TextFieldName)
                    .dataType(DataType.VarChar)
                    .maxLength(1024)
                    .build());

            List<IndexParam> indexes = new ArrayList<>();
            indexes.add(IndexParam.builder()
                    .fieldName(VectorFieldName)
                    .indexType(IndexParam.IndexType.FLAT)
                    .metricType(IndexParam.MetricType.COSINE)
                    .build());

            CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                    .collectionName(CollectionName)
                    .collectionSchema(collectionSchema)
                    .indexParams(indexes)
                    .consistencyLevel(ConsistencyLevel.BOUNDED)
                    .build();
            client.createCollection(requestCreate);

            insertData(rowCount);
        } finally {
            pool.returnClient(DemoKey, client);
        }
    }

    private static void insertData(long rowCount) {
        System.out.println("========== insertData() ==========");
        MilvusClientV2 client = null;
        try {
            client = pool.getClient(DemoKey);
            if (client == null) {
                System.out.println("Cannot not get client from key:" + DemoKey);
                return;
            }

            Gson gson = new Gson();
            long inserted = 0L;
            while (inserted < rowCount) {
                long batch = 1000L;
                if (rowCount - inserted < batch) {
                    batch = rowCount - inserted;
                }
                List<JsonObject> rows = new ArrayList<>();
                for (long i = 0; i < batch; i++) {
                    JsonObject row = new JsonObject();
                    row.add(VectorFieldName, gson.toJsonTree(CommonUtils.generateFloatVector(DIM)));
                    row.addProperty(TextFieldName, "text_" + i);
                    rows.add(row);
                }
                InsertResp resp = client.insert(InsertReq.builder()
                        .collectionName(CollectionName)
                        .data(rows)
                        .build());
                inserted += resp.getInsertCnt();
                System.out.println("Inserted count:" + resp.getInsertCnt());
            }

            QueryResp countR = client.query(QueryReq.builder()
                    .collectionName(CollectionName)
                    .outputFields(Collections.singletonList("count(*)"))
                    .consistencyLevel(ConsistencyLevel.STRONG)
                    .build());
            System.out.printf("%d rows persisted%n", (long) countR.getQueryResults().get(0).getEntity().get("count(*)"));
        } finally {
            pool.returnClient(DemoKey, client);
        }
    }

    private static void search() {
        MilvusClientV2 client = null;
        try {
            client = pool.getClient(DemoKey);
            while (client == null) {
                try {
                    // getClient() might return null if it exceeds the borrowMaxWaitMillis when the pool is full.
                    // retry to call until it return a client.
                    client = pool.getClient(DemoKey);
                } catch (Exception e) {
                    System.out.printf("Failed to get client, will retry, error: %s%n", e.getMessage());
                }
            }

//            long start = System.currentTimeMillis();
            FloatVec vector = new FloatVec(CommonUtils.generateFloatVector(DIM));
            SearchResp resp = client.search(SearchReq.builder()
                    .collectionName(CollectionName)
                    .limit(10)
                    .data(Collections.singletonList(vector))
                    .annsField(VectorFieldName)
                    .outputFields(Collections.singletonList(TextFieldName))
                    .build());
//            System.out.printf("search time cost: %dms%n", System.currentTimeMillis() - start);
        } finally {
            pool.returnClient(DemoKey, client);
        }
    }

    private static void printPoolState() {
        System.out.println("========== printPoolState() ==========");
        System.out.printf("%d idle clients and %d active clients%n",
                pool.getIdleClientNumber(DemoKey), pool.getActiveClientNumber(DemoKey));
        System.out.printf("%.2f clients fetched per second%n", pool.fetchClientPerSecond(DemoKey));
    }

    private static void concurrentSearch(int threadCount, int requestCount) {
        System.out.println("\n======================================================================");
        System.out.println("======================= ConcurrentSearch =============================");
        System.out.println("======================================================================");

        AtomicLong totalTimeCostMs = new AtomicLong(0L);
        class Worker implements Runnable {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                search();
                long end = System.currentTimeMillis();
                totalTimeCostMs.addAndGet(end - start);
            }
        }

        try {
            long start = System.currentTimeMillis();
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < requestCount; i++) {
                Runnable worker = new Worker();
                executor.execute(worker);
            }
            executor.shutdown();

            // with requests start, more active clients will be created
            boolean done = false;
            while (!done) {
                printPoolState();
                done = executor.awaitTermination(1, TimeUnit.SECONDS);
            }

            long timeGapMs = System.currentTimeMillis() - start;
            float avgQPS = (float) requestCount * 1000 / timeGapMs;
            long avgLatency = totalTimeCostMs.get() / requestCount;
            System.out.printf("%n%d requests done in %.1f seconds, average QPS: %.1f, average latency: %dms%n%n",
                    requestCount, (float) timeGapMs / 1000, avgQPS, avgLatency);

            // after all requests are done, the active clients will be retired and eventually only one idle client left.
            // just demo the pool can automatically destroy idle clients, you can directly close the pool without waiting
            // it in practice.
            while (pool.getActiveClientNumber(DemoKey) > 1) {
                TimeUnit.SECONDS.sleep(1);
                printPoolState();
            }
        } catch (Exception e) {
            System.err.println("Failed to create executor: " + e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        long rowCount = 10000;
        createCollection(true, rowCount);

        int threadCount = 50;
        int requestCount = 10000;
        concurrentSearch(threadCount, requestCount);

        // do again
        threadCount = 100;
        requestCount = 20000;
        concurrentSearch(threadCount, requestCount);

        pool.close();
    }
}

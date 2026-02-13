package io.milvus.benchmark;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.pool.MilvusClientV2Pool;
import io.milvus.pool.PoolConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.common.IndexParam;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class PoolBenchmark extends BenchmarkBase {

    private static final String COLLECTION_NAME = "java_sdk_pool_benchmark";
    private static final int INSERT_BATCH_SIZE = 1000;
    private static final String POOL_KEY = "default";

    private int vectorDim = 128;
    private int baseRowCount = 100_000;
    private int benchmarkRows = 1000;
    private int threadCount = 32;
    private int requestCount = 10_000;
    private int searchTopK = 10;
    private int rounds = 10;

    private int maxIdlePerKey = 10;
    private int maxTotalPerKey = 20;
    private int maxTotal = 100;
    private int maxBlockWaitSeconds = 30;

    private final Map<String, List<BenchmarkResult>> allResults = new LinkedHashMap<>();

    public static void main(String[] args) throws Exception {
        new PoolBenchmark().execute(args);
    }

    @Override
    protected String name() {
        return "Milvus Java SDK Pool Benchmark";
    }

    @Override
    protected void applyConfig(JsonObject config) {
        if (config.has("vectorDim")) vectorDim = config.get("vectorDim").getAsInt();
        if (config.has("rowCount")) baseRowCount = config.get("rowCount").getAsInt();
        if (config.has("threadCount")) threadCount = config.get("threadCount").getAsInt();
        if (config.has("requestCount")) requestCount = config.get("requestCount").getAsInt();
        if (config.has("topK")) searchTopK = config.get("topK").getAsInt();
        if (config.has("rounds")) rounds = config.get("rounds").getAsInt();
        if (config.has("maxIdlePerKey")) maxIdlePerKey = config.get("maxIdlePerKey").getAsInt();
        if (config.has("maxTotalPerKey")) maxTotalPerKey = config.get("maxTotalPerKey").getAsInt();
        if (config.has("maxTotal")) maxTotal = config.get("maxTotal").getAsInt();
        if (config.has("maxBlockWaitSeconds")) maxBlockWaitSeconds = config.get("maxBlockWaitSeconds").getAsInt();
    }

    @Override
    protected void prepare() {
        System.out.println("[Setup] Creating collection and inserting base data...");
        MilvusClientV2 setupClient = new MilvusClientV2(connectConfig());

        // Drop if exists
        if (setupClient.hasCollection(HasCollectionReq.builder()
                .collectionName(COLLECTION_NAME).build())) {
            setupClient.dropCollection(DropCollectionReq.builder()
                    .collectionName(COLLECTION_NAME).build());
        }

        // Quick-create collection with FLAT index
        IndexParam indexParam = IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build();
        setupClient.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .dimension(vectorDim)
                .autoID(Boolean.TRUE)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        System.out.println("[Setup] Collection created: " + COLLECTION_NAME);

        // Batch insert base rows
        Gson gson = new Gson();
        int inserted = 0;
        while (inserted < baseRowCount) {
            int batchSize = Math.min(INSERT_BATCH_SIZE, baseRowCount - inserted);
            List<JsonObject> rows = new ArrayList<>(batchSize);
            for (int j = 0; j < batchSize; j++) {
                JsonObject row = new JsonObject();
                row.add("vector", gson.toJsonTree(generateFloatVector(vectorDim)));
                rows.add(row);
            }
            setupClient.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
            inserted += batchSize;
        }
        System.out.println("[Setup] Inserted " + inserted + " rows");
        setupClient.close();
        System.out.println("[Setup] Setup complete");
        System.out.println();
    }

    @Override
    protected void run() {
        System.out.println("Rounds: " + rounds);
        System.out.println();

        for (int round = 1; round <= rounds; round++) {
            System.out.println("======== Round " + round + " / " + rounds + " ========");

            try {
                List<BenchmarkResult> noPoolResults = noPoolBenchmark();
                List<BenchmarkResult> poolResults = poolBenchmark();

                for (BenchmarkResult r : noPoolResults) {
                    allResults.computeIfAbsent(r.name, k -> new ArrayList<>()).add(r);
                }
                for (BenchmarkResult r : poolResults) {
                    allResults.computeIfAbsent(r.name, k -> new ArrayList<>()).add(r);
                }
            } catch (Exception e) {
                System.out.println("Error in round " + round + ": " + e.getMessage());
            }
        }
    }

    @Override
    protected void printSummary() {
        System.out.println("=== Benchmark Summary (" + rounds + " rounds) ===");

        StringBuilder md = new StringBuilder();
        md.append("# PoolBenchmark Results\n\n");
        md.append("- **Date**: ").append(timestamp()).append("\n");
        md.append("- **SDK Version**: ").append(sdkVersion).append("\n");
        md.append("- **URI**: ").append(uri).append("\n");
        md.append("- **Rows**: ").append(baseRowCount).append("\n");
        md.append("- **Repeats**: ").append(rounds).append("\n");
        md.append("- **Threads**: ").append(threadCount).append("\n");
        md.append("- **Requests**: ").append(requestCount).append(" (insert: ").append(benchmarkRows).append(")\n\n");
        md.append("## Collection Schema\n\n");
        md.append("Collection: `").append(COLLECTION_NAME).append("`\n\n");
        md.append("| Field | Type | Details |\n");
        md.append("|-------|------|---------|\n");
        md.append("| id | Int64 | Primary key, autoID |\n");
        md.append("| vector | FloatVector | dim=").append(vectorDim).append(", FLAT/COSINE |\n\n");
        md.append("## Results\n\n");

        // Group results by operation type (Search, Query, Insert)
        String[] operations = {"Search", "Query", "Insert"};
        for (String op : operations) {
            System.out.println();
            System.out.println("--- " + op + " ---");
            String header = String.format("  %-20s | %6s | %14s | %14s | %14s | %6s",
                    "Mode", "Rounds", "Avg Total Time", "Avg Latency", "Avg QPS", "Errors");
            String separator = new String(new char[header.length()]).replace('\0', '-');
            System.out.println(separator);
            System.out.println(header);
            System.out.println(separator);

            md.append("### ").append(op).append("\n\n");
            md.append("| Mode | Rounds | Avg Total Time | Avg Latency | Avg QPS | Errors |\n");
            md.append("|------|--------|---------------|-------------|---------|--------|\n");

            for (Map.Entry<String, List<BenchmarkResult>> entry : allResults.entrySet()) {
                String name = entry.getKey();
                if (!name.startsWith(op)) {
                    continue;
                }
                List<BenchmarkResult> runs = entry.getValue();
                int rounds = runs.size();
                long totalErrors = 0;
                double sumTotalTimeMs = 0;
                double sumAvgLatencyMs = 0;
                double sumQPS = 0;
                for (BenchmarkResult r : runs) {
                    totalErrors += r.errorCount;
                    sumTotalTimeMs += r.totalTimeMs;
                    sumAvgLatencyMs += r.getAvgLatencyMs();
                    sumQPS += r.getQPS();
                }
                // Extract mode (e.g. "No-Pool" or "Pool") from name
                String mode = name.contains("(") ? name.substring(name.indexOf('(')) : name;
                System.out.printf("  %-20s | %6d | %11.0f ms | %11.1f ms | %11.1f/s | %6d%n",
                        mode, rounds,
                        sumTotalTimeMs / rounds,
                        sumAvgLatencyMs / rounds,
                        sumQPS / rounds,
                        totalErrors);
                md.append(String.format("| %s | %d | %.0f ms | %.1f ms | %.1f/s | %d |\n",
                        mode, rounds,
                        sumTotalTimeMs / rounds,
                        sumAvgLatencyMs / rounds,
                        sumQPS / rounds,
                        totalErrors));
            }
            System.out.println(separator);
            md.append("\n");
        }

        writeResultsFile(md.toString());
    }

    private List<BenchmarkResult> noPoolBenchmark() throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();

        System.out.println("[No-Pool] Starting benchmarks with a single shared client...");
        MilvusClientV2 sharedClient = new MilvusClientV2(connectConfig());

        results.add(runBenchmark("Search (No-Pool)", threadCount, requestCount, () -> {
            sharedClient.search(SearchReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(new FloatVec(generateFloatVector(vectorDim))))
                    .limit(searchTopK)
                    .build());
        }));

        results.add(runBenchmark("Query  (No-Pool)", threadCount, requestCount, () -> {
            long randomId = ThreadLocalRandom.current().nextLong(1, baseRowCount);
            sharedClient.query(QueryReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .filter("id > " + randomId)
                    .limit(10)
                    .build());
        }));

        results.add(runBenchmark("Insert (No-Pool)", threadCount, benchmarkRows, () -> {
            Gson g = new Gson();
            JsonObject row = new JsonObject();
            row.add("vector", g.toJsonTree(generateFloatVector(vectorDim)));
            sharedClient.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(row))
                    .build());
        }));

        sharedClient.close();
        System.out.println("[No-Pool] Done");
        System.out.println();

        return results;
    }

    private List<BenchmarkResult> poolBenchmark() throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();

        System.out.println("[Pool] Starting benchmarks with MilvusClientV2Pool...");
        PoolConfig poolConfig = PoolConfig.builder()
                .maxIdlePerKey(maxIdlePerKey)
                .maxTotalPerKey(maxTotalPerKey)
                .maxTotal(maxTotal)
                .maxBlockWaitDuration(Duration.ofSeconds(maxBlockWaitSeconds))
                .build();
        MilvusClientV2Pool pool = new MilvusClientV2Pool(poolConfig, connectConfig());
        try {
            // preparePool() may not exist in older SDK versions, call via reflection
            java.lang.reflect.Method m = pool.getClass().getMethod("preparePool", String.class);
            m.invoke(pool, POOL_KEY);
        } catch (NoSuchMethodException e) {
            // not available in this SDK version, skip
        }

        results.add(runBenchmark("Search (Pool)", threadCount, requestCount, () -> {
            MilvusClientV2 client = pool.getClient(POOL_KEY);
            try {
                client.search(SearchReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .data(Collections.singletonList(new FloatVec(generateFloatVector(vectorDim))))
                        .limit(searchTopK)
                        .build());
            } finally {
                pool.returnClient(POOL_KEY, client);
            }
        }));

        results.add(runBenchmark("Query  (Pool)", threadCount, requestCount, () -> {
            long randomId = ThreadLocalRandom.current().nextLong(1, baseRowCount);
            MilvusClientV2 client = pool.getClient(POOL_KEY);
            try {
                client.query(QueryReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .filter("id > " + randomId)
                        .limit(10)
                        .build());
            } finally {
                pool.returnClient(POOL_KEY, client);
            }
        }));

        results.add(runBenchmark("Insert (Pool)", threadCount, benchmarkRows, () -> {
            Gson g = new Gson();
            JsonObject row = new JsonObject();
            row.add("vector", g.toJsonTree(generateFloatVector(vectorDim)));
            MilvusClientV2 client = pool.getClient(POOL_KEY);
            try {
                client.insert(InsertReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .data(Collections.singletonList(row))
                        .build());
            } finally {
                pool.returnClient(POOL_KEY, client);
            }
        }));

        pool.close();
        System.out.println("[Pool] Done");
        System.out.println();

        return results;
    }

    private static BenchmarkResult runBenchmark(String name, int threadCount, int requestCount,
                                                 Runnable task) throws InterruptedException {
        System.out.println("  Running: " + name + " (" + requestCount + " requests, "
                + threadCount + " threads)");

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicLong totalLatencyMs = new AtomicLong(0);
        AtomicLong errorCount = new AtomicLong(0);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < requestCount; i++) {
            executor.execute(() -> {
                long begin = System.currentTimeMillis();
                try {
                    task.run();
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                }
                totalLatencyMs.addAndGet(System.currentTimeMillis() - begin);
            });
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
        long totalTimeMs = System.currentTimeMillis() - startTime;

        BenchmarkResult result = new BenchmarkResult(name, totalTimeMs, totalLatencyMs.get(),
                requestCount, errorCount.get());
        System.out.println("    " + result.toSummary());
        return result;
    }

    static class BenchmarkResult {
        final String name;
        final long totalTimeMs;
        final long totalLatencyMs;
        final long requestCount;
        final long errorCount;

        BenchmarkResult(String name, long totalTimeMs, long totalLatencyMs,
                        long requestCount, long errorCount) {
            this.name = name;
            this.totalTimeMs = totalTimeMs;
            this.totalLatencyMs = totalLatencyMs;
            this.requestCount = requestCount;
            this.errorCount = errorCount;
        }

        double getAvgLatencyMs() {
            return requestCount > 0 ? (double) totalLatencyMs / requestCount : 0;
        }

        double getQPS() {
            return totalTimeMs > 0 ? (double) requestCount / totalTimeMs * 1000 : 0;
        }

        String toSummary() {
            return String.format("%.1f ms avg, %.1f QPS, %d errors",
                    getAvgLatencyMs(), getQPS(), errorCount);
        }
    }
}

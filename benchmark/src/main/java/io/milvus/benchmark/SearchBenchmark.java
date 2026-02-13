package io.milvus.benchmark;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.common.utils.Float16Utils;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.request.data.Float16Vec;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SearchBenchmark extends BenchmarkBase {

    private static final String COLLECTION_NAME = "java_sdk_search_benchmark";
    private static final int ID_START = 100_000;
    private static final int INSERT_BATCH_SIZE = 1000;

    private int floatVectorDim = 768;
    private int binaryVectorDim = 2048;
    private int float16VectorDim = 768;
    private int rowCount = 100_000;
    private int repeat = 10;

    private static final String[] WORD_POOL = {
            "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
            "hello", "world", "milvus", "vector", "database", "search", "engine",
            "machine", "learning", "deep", "neural", "network", "artificial",
            "intelligence", "data", "science", "big", "cloud", "computing",
            "algorithm", "performance", "benchmark", "index", "query", "insert",
            "collection", "field", "schema", "primary", "key", "dimension",
            "sparse", "dense", "binary", "float", "integer", "string"
    };

    private static final String[] NAMES = {
            "Tom", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace"
    };

    private static final List<String> ALL_FIELDS = Arrays.asList(
            "id", "float_vector", "binary_vector", "float16_vector",
            "text", "array_double", "json"
    );

    private MilvusClientV2 client;
    private double queryAvgLatency;
    private final Map<String, List<String>> resultLinesByField = new LinkedHashMap<>();
    private final Map<String, List<String>> mdResultLinesByField = new LinkedHashMap<>();

    public static void main(String[] args) {
        new SearchBenchmark().execute(args);
    }

    @Override
    protected String name() {
        return "Milvus Java SDK Search Benchmark";
    }

    @Override
    protected void applyConfig(JsonObject config) {
        if (config.has("floatVectorDim")) floatVectorDim = config.get("floatVectorDim").getAsInt();
        if (config.has("binaryVectorDim")) binaryVectorDim = config.get("binaryVectorDim").getAsInt();
        if (config.has("float16VectorDim")) float16VectorDim = config.get("float16VectorDim").getAsInt();
        if (config.has("rowCount")) rowCount = config.get("rowCount").getAsInt();
        if (config.has("repeat")) repeat = config.get("repeat").getAsInt();
    }

    @Override
    protected void prepare() {
        client = new MilvusClientV2(connectConfig());

        boolean exists = client.hasCollection(HasCollectionReq.builder()
                .collectionName(COLLECTION_NAME).build());
        if (exists) {
            System.out.println("[Setup] Collection already exists, skipping creation and insertion.");
            System.out.println();
            return;
        }

        System.out.println("[Setup] Creating collection: " + COLLECTION_NAME);

        // Build schema
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder().build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(floatVectorDim)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(binaryVectorDim)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("float16_vector")
                .dataType(DataType.Float16Vector)
                .dimension(float16VectorDim)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .enableAnalyzer(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("array_double")
                .dataType(DataType.Array)
                .elementType(DataType.Double)
                .maxCapacity(100)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("json")
                .dataType(DataType.JSON)
                .build());

        // Add BM25 function
        schema.addFunction(CreateCollectionReq.Function.builder()
                .name("bm25_func")
                .functionType(FunctionType.BM25)
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("sparse_vector"))
                .build());

        // Build indexes
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("float16_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("binary_vector")
                .indexType(IndexParam.IndexType.BIN_FLAT)
                .metricType(IndexParam.MetricType.HAMMING)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName("sparse_vector")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build());
        System.out.println("[Setup] Collection created.");

        // Insert data
        System.out.println("[Setup] Inserting " + rowCount + " rows...");
        Gson gson = new Gson();
        int inserted = 0;
        while (inserted < rowCount) {
            int batchSize = Math.min(INSERT_BATCH_SIZE, rowCount - inserted);
            List<JsonObject> rows = new ArrayList<>(batchSize);
            for (int j = 0; j < batchSize; j++) {
                int id = ID_START + inserted + j;
                JsonObject row = new JsonObject();
                row.addProperty("id", id);
                row.add("float_vector", gson.toJsonTree(generateFloatVector(floatVectorDim)));
                row.add("binary_vector", gson.toJsonTree(generateBinaryVector(binaryVectorDim)));

                List<Float> f16Floats = generateFloatVector(float16VectorDim);
                ByteBuffer buf = Float16Utils.f32VectorToFp16Buffer(f16Floats);
                row.add("float16_vector", gson.toJsonTree(buf.array()));

                // sparse_vector is auto-generated by BM25 function from text
                row.addProperty("text", generateRandomSentence());
                row.add("array_double", gson.toJsonTree(generateArrayDouble(100)));
                row.add("json", gson.toJsonTree(generateJsonField()));

                rows.add(row);
            }
            client.insert(InsertReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(rows)
                    .build());
            inserted += batchSize;
            if (inserted % 10000 == 0) {
                System.out.println("[Setup] Inserted " + inserted + " / " + rowCount + " rows");
            }
        }
        System.out.println("[Setup] Insertion complete.");
        System.out.println();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void run() {
        // Query benchmark — retrieve vectors and texts for search
        System.out.println("=== Query Benchmark ===");
        String filter = "id in [100000, 110000, 120000, 130000, 140000, 150000, 160000, 170000, 180000, 190000]";

        List<List<Float>> floatVecs = new ArrayList<>();
        List<ByteBuffer> binaryVecs = new ArrayList<>();
        List<ByteBuffer> float16Vecs = new ArrayList<>();
        List<String> texts = new ArrayList<>();

        long totalLatencyMs = 0;
        for (int i = 0; i < repeat; i++) {
            long start = System.currentTimeMillis();
            QueryResp resp = client.query(QueryReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .filter(filter)
                    .outputFields(Collections.singletonList("*"))
                    .build());
            long elapsed = System.currentTimeMillis() - start;
            totalLatencyMs += elapsed;
            System.out.printf("  Query iteration %d: %d ms, %d results%n",
                    i + 1, elapsed, resp.getQueryResults().size());

            // Save data from first iteration for search benchmarks
            if (i == 0) {
                for (QueryResp.QueryResult result : resp.getQueryResults()) {
                    Map<String, Object> entity = result.getEntity();

                    Object fv = entity.get("float_vector");
                    if (fv instanceof List) {
                        floatVecs.add((List<Float>) fv);
                    }

                    Object bv = entity.get("binary_vector");
                    if (bv instanceof ByteBuffer) {
                        binaryVecs.add((ByteBuffer) bv);
                    }

                    Object f16v = entity.get("float16_vector");
                    if (f16v instanceof ByteBuffer) {
                        float16Vecs.add((ByteBuffer) f16v);
                    }

                    Object txt = entity.get("text");
                    if (txt instanceof String) {
                        texts.add((String) txt);
                    }
                }
            }
        }
        queryAvgLatency = (double) totalLatencyMs / repeat;
        System.out.printf("  Query avg latency: %.1f ms%n", queryAvgLatency);
        System.out.printf("  Retrieved: %d float_vectors, %d binary_vectors, %d float16_vectors, %d texts%n",
                floatVecs.size(), binaryVecs.size(), float16Vecs.size(), texts.size());
        System.out.println();

        // Search benchmark — 4 vector fields x 4 variants
        System.out.println("=== Search Benchmark ===");

        // Build search data for each vector field
        Map<String, List<BaseVector>> searchDataMap = new LinkedHashMap<>();

        if (!floatVecs.isEmpty()) {
            List<BaseVector> data = new ArrayList<>();
            for (List<Float> vec : floatVecs) {
                data.add(new FloatVec(vec));
            }
            searchDataMap.put("float_vector", data);
        }

        if (!binaryVecs.isEmpty()) {
            List<BaseVector> data = new ArrayList<>();
            for (ByteBuffer buf : binaryVecs) {
                data.add(new BinaryVec(buf));
            }
            searchDataMap.put("binary_vector", data);
        }

        if (!float16Vecs.isEmpty()) {
            List<BaseVector> data = new ArrayList<>();
            for (ByteBuffer buf : float16Vecs) {
                data.add(new Float16Vec(buf));
            }
            searchDataMap.put("float16_vector", data);
        }

        if (!texts.isEmpty()) {
            List<BaseVector> data = new ArrayList<>();
            for (String text : texts) {
                data.add(new EmbeddedText(text));
            }
            searchDataMap.put("sparse_vector", data);
        }

        if (searchDataMap.isEmpty()) {
            System.out.println("  No search data available from query, skipping search benchmark.");
            return;
        }

        // 4 topK/outputFields variants
        String[][] variants = {
                {"topK=10, no output fields", "10", "none"},
                {"topK=10, all output fields", "10", "all"},
                {"topK=1000, no output fields", "1000", "none"},
                {"topK=1000, all output fields", "1000", "all"},
        };

        for (Map.Entry<String, List<BaseVector>> entry : searchDataMap.entrySet()) {
            String annsField = entry.getKey();
            List<BaseVector> searchData = entry.getValue();

            System.out.println();
            System.out.println("--- " + annsField + " (nq=" + searchData.size() + ") ---");

            for (String[] variant : variants) {
                String variantLabel = variant[0];
                int topK = Integer.parseInt(variant[1]);
                boolean allFields = "all".equals(variant[2]);

                long variantLatencyMs = 0;
                long totalResults = 0;
                for (int i = 0; i < repeat; i++) {
                    SearchReq.SearchReqBuilder builder = SearchReq.builder()
                            .collectionName(COLLECTION_NAME)
                            .annsField(annsField)
                            .data(searchData)
                            .limit(topK);
                    if (allFields) {
                        builder.outputFields(ALL_FIELDS);
                    }

                    long start = System.currentTimeMillis();
                    SearchResp resp = client.search(builder.build());
                    long elapsed = System.currentTimeMillis() - start;
                    variantLatencyMs += elapsed;
                    totalResults = resp.getSearchResults().size();
                }
                double variantAvg = (double) variantLatencyMs / repeat;
                String line = String.format("  %-25s  avg: %8.1f ms  (nq=%d, results=%d)",
                        variantLabel, variantAvg, searchData.size(), totalResults);
                System.out.println(line);
                resultLinesByField.computeIfAbsent(annsField, k -> new ArrayList<>()).add(line);
                mdResultLinesByField.computeIfAbsent(annsField, k -> new ArrayList<>())
                        .add(String.format("| %s | %.1f ms | %d | %d |",
                                variantLabel, variantAvg, searchData.size(), totalResults));
            }
        }
    }

    @Override
    protected void printSummary() {
        System.out.println();
        System.out.println("=== Search Benchmark Summary ===");
        for (Map.Entry<String, List<String>> entry : resultLinesByField.entrySet()) {
            System.out.println();
            System.out.println("--- " + entry.getKey() + " ---");
            for (String line : entry.getValue()) {
                System.out.println(line);
            }
        }

        StringBuilder md = new StringBuilder();
        md.append("# SearchBenchmark Results\n\n");
        md.append("- **Date**: ").append(timestamp()).append("\n");
        md.append("- **SDK Version**: ").append(sdkVersion).append("\n");
        md.append("- **URI**: ").append(uri).append("\n");
        md.append("- **Rows**: ").append(rowCount).append("\n");
        md.append("- **Repeats**: ").append(repeat).append("\n\n");
        md.append("## Collection Schema\n\n");
        md.append("Collection: `").append(COLLECTION_NAME).append("`\n\n");
        md.append("| Field | Type | Details |\n");
        md.append("|-------|------|---------|\n");
        md.append("| id | Int64 | Primary key |\n");
        md.append("| float_vector | FloatVector | dim=").append(floatVectorDim).append(", FLAT/COSINE |\n");
        md.append("| binary_vector | BinaryVector | dim=").append(binaryVectorDim).append(", BIN_FLAT/HAMMING |\n");
        md.append("| float16_vector | Float16Vector | dim=").append(float16VectorDim).append(", FLAT/COSINE |\n");
        md.append("| sparse_vector | SparseFloatVector | SPARSE_INVERTED_INDEX/BM25 (auto-generated) |\n");
        md.append("| text | VarChar | maxLength=65535, analyzer enabled |\n");
        md.append("| array_double | Array(Double) | maxCapacity=100 |\n");
        md.append("| json | JSON | - |\n\n");
        md.append("**Function**: BM25 (`text` → `sparse_vector`)\n\n");
        md.append("## Query Benchmark\n\n");
        md.append(String.format("- Avg latency: %.1f ms\n\n", queryAvgLatency));
        md.append("## Search Benchmark\n\n");
        for (Map.Entry<String, List<String>> entry : mdResultLinesByField.entrySet()) {
            md.append("### ").append(entry.getKey()).append("\n\n");
            md.append("| Variant | Avg Latency | nq | Results |\n");
            md.append("|---------|-------------|----|---------|\n");
            for (String line : entry.getValue()) {
                md.append(line).append("\n");
            }
            md.append("\n");
        }

        writeResultsFile(md.toString());

        client.close();
    }

    // --- Helper methods ---

    private static byte[] generateBinaryVector(int dim) {
        byte[] bytes = new byte[dim / 8];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }

    private static String generateRandomSentence() {
        Random random = ThreadLocalRandom.current();
        int wordCount = 5 + random.nextInt(11); // 5 to 15 words
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) sb.append(' ');
            sb.append(WORD_POOL[random.nextInt(WORD_POOL.length)]);
        }
        return sb.toString();
    }

    private static List<Double> generateArrayDouble(int maxLen) {
        Random random = ThreadLocalRandom.current();
        int len = random.nextInt(maxLen + 1); // 0 to maxLen
        List<Double> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(random.nextDouble() * 1000);
        }
        return list;
    }

    private static JsonObject generateJsonField() {
        Random random = ThreadLocalRandom.current();
        JsonObject json = new JsonObject();

        // Random date
        int year = 2020 + random.nextInt(6);
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28);
        json.addProperty("date", String.format("%d-%02d-%02d", year, month, day));

        // Random temperature
        json.addProperty("temperature", -10 + random.nextInt(51)); // -10 to 40

        // Meeting sub-object
        JsonObject meeting = new JsonObject();
        int memberCount = 1 + random.nextInt(4);
        String[] members = new String[memberCount];
        for (int i = 0; i < memberCount; i++) {
            members[i] = NAMES[random.nextInt(NAMES.length)];
        }
        Gson gson = new Gson();
        meeting.add("member", gson.toJsonTree(members));
        int hour = 8 + random.nextInt(14); // 8 to 21
        int minute = random.nextInt(4) * 15; // 0, 15, 30, 45
        meeting.addProperty("time", String.format("%02d:%02d PM", hour, minute));
        json.add("meeting", meeting);

        return json;
    }
}

package io.milvus.v2;

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddCollectionFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class NullableVectorExample {
    private static final int DIMENSION = 8;
    private static final Random RANDOM = new Random();

    private static List<Float> generateFloatVector() {
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < DIMENSION; i++) {
            vector.add(RANDOM.nextFloat());
        }
        return vector;
    }

    public static void main(String[] args) throws InterruptedException {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);
        System.out.println("Connected to Milvus\n");

        insertNullVectors(client);
        addNullableVectorField(client);

        client.close(5L);
        System.out.println("Done!");
    }

    private static void insertNullVectors(MilvusClientV2 client) throws InterruptedException {
        String collectionName = "java_sdk_example_insert_null_vectors";
        System.out.println("=== Demo 1: Insert null vectors ===");

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Create collection with nullable vector field
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("name")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("embedding")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .isNullable(true)  // Enable nullable for vector field
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
        System.out.println("Created collection with nullable vector field");

        // Create index
        IndexParam indexParam = IndexParam.builder()
                .fieldName("embedding")
                .metricType(IndexParam.MetricType.L2)
                .indexType(IndexParam.IndexType.FLAT)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());

        // Load collection
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Prepare test data: 100 rows, ~50% null vectors
        int totalRows = 100;
        int nullPercent = 50;
        List<JsonObject> data = new ArrayList<>();
        int nullCount = 0;
        int validCount = 0;

        for (int i = 1; i <= totalRows; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", (long) i);
            row.addProperty("name", "item_" + i);

            boolean isNull = RANDOM.nextInt(100) < nullPercent;
            if (isNull) {
                row.add("embedding", JsonNull.INSTANCE);
                nullCount++;
            } else {
                row.add("embedding", JsonUtils.toJsonTree(generateFloatVector()));
                validCount++;
            }
            data.add(row);
        }

        // Insert data
        InsertResp insertResp = client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
        System.out.println("Inserted " + insertResp.getInsertCnt() + " rows: " + validCount + " valid, " + nullCount + " null");

        Thread.sleep(1000);

        // Query all data
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("id >= 0")
                .outputFields(Arrays.asList("id", "embedding"))
                .limit(totalRows + 10)
                .build());

        int queryNullCount = 0;
        int queryValidCount = 0;
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Object embedding = result.getEntity().get("embedding");
            if (embedding == null) {
                queryNullCount++;
            } else {
                queryValidCount++;
            }
        }
        System.out.println("Query result: " + queryValidCount + " valid, " + queryNullCount + " null");

        // Search - only returns non-null vectors
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(new FloatVec(generateFloatVector())))
                .annsField("embedding")
                .topK(10)
                .outputFields(Arrays.asList("id", "embedding"))
                .build());

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        if (!searchResults.isEmpty()) {
            System.out.println("Search returned " + searchResults.get(0).size() + " hits (only non-null vectors)");
        }

        // Cleanup
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.println("Dropped collection\n");
    }

    private static void addNullableVectorField(MilvusClientV2 client) throws InterruptedException {
        String collectionName = "java_sdk_example_add_vector_field";
        System.out.println("=== Demo 2: Add nullable vector field to existing collection ===");

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Create collection with one vector field (Milvus requires at least one)
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("name")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("embedding_v1")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());

        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .build());
        System.out.println("Created collection with one vector field");

        // Create index and load
        IndexParam indexParam = IndexParam.builder()
                .fieldName("embedding_v1")
                .metricType(IndexParam.MetricType.L2)
                .indexType(IndexParam.IndexType.FLAT)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Insert some data first
        List<JsonObject> data = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", (long) i);
            row.addProperty("name", "item_" + i);
            row.add("embedding_v1", JsonUtils.toJsonTree(generateFloatVector()));
            data.add(row);
        }
        client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(data)
                .build());
        System.out.println("Inserted 10 rows");

        // Release before adding field
        client.releaseCollection(ReleaseCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // Add a second nullable vector field to existing collection
        client.addCollectionField(AddCollectionFieldReq.builder()
                .collectionName(collectionName)
                .fieldName("embedding_v2")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .isNullable(true)  // Must be nullable when adding to existing collection
                .build());
        System.out.println("Added nullable vector field 'embedding_v2'");

        // Create index for the new field
        IndexParam newIndexParam = IndexParam.builder()
                .fieldName("embedding_v2")
                .metricType(IndexParam.MetricType.L2)
                .indexType(IndexParam.IndexType.FLAT)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .indexParams(Collections.singletonList(newIndexParam))
                .build());

        // Load collection
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        Thread.sleep(1000);

        // Query to verify old rows have null for the new field
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("id >= 0")
                .outputFields(Arrays.asList("id", "embedding_v1", "embedding_v2"))
                .limit(10)
                .build());

        System.out.println("Query result (old rows have null for new field):");
        for (QueryResp.QueryResult result : queryResp.getQueryResults()) {
            Map<String, Object> entity = result.getEntity();
            long id = (Long) entity.get("id");
            Object v1 = entity.get("embedding_v1");
            Object v2 = entity.get("embedding_v2");
            System.out.println("  id=" + id + ", embedding_v1=" + (v1 == null ? "null" : "has value")
                    + ", embedding_v2=" + (v2 == null ? "null" : "has value"));
        }

        // Cleanup
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.println("Dropped collection\n");
    }
}

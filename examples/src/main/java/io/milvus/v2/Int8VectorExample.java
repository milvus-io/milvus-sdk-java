package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.request.data.Int8Vec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.nio.ByteBuffer;
import java.util.*;

public class Int8VectorExample {
    private static final String COLLECTION_NAME = "java_sdk_example_int8_vector_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";

    private static final Integer VECTOR_DIM = 128;

    private static List<ByteBuffer> generateInt8Vectors(int count) {
        Random RANDOM = new Random();
        List<ByteBuffer> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ByteBuffer vector = ByteBuffer.allocate(VECTOR_DIM);
            for (int k = 0; k < VECTOR_DIM; ++k) {
                vector.put((byte) (RANDOM.nextInt(256) - 128));
            }
            vectors.add(vector);
        }

        return vectors;
    }


    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(ID_FIELD)
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.Int8Vector)
                .dimension(VECTOR_DIM)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("M", 64);
        extraParams.put("efConstruction", 200);
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert entities by rows
        int rowCount = 10000;
        List<ByteBuffer> vectors = generateInt8Vectors(rowCount);
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (long i = 0L; i < rowCount; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            ByteBuffer vector = vectors.get((int)i);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector.array()));
            rows.add(row);
        }

        client.insert(InsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(rows)
                .build());

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // Pick some vectors from the inserted vectors to search
        // Ensure the returned top1 item's ID should be equal to target vector's ID
        for (int i = 0; i < 10; i++) {
            Random ran = new Random();
            int k = ran.nextInt(rowCount);
            ByteBuffer targetVector = vectors.get(k);
            SearchResp searchResp = client.search(SearchReq.builder()
                    .collectionName(COLLECTION_NAME)
                    .data(Collections.singletonList(new Int8Vec(targetVector)))
                    .annsField(VECTOR_FIELD)
                    .outputFields(Collections.singletonList(VECTOR_FIELD))
                    .limit(3)
                    .build());

            // The search() allows multiple target vectors to search in a batch.
            // Here we only input one vector to search, get the result of No.0 vector to check
            List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
            List<SearchResp.SearchResult> results = searchResults.get(0);
            System.out.printf("\nThe result of No.%d vector %s:\n", k, Arrays.toString(targetVector.array()));
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
                ByteBuffer vector = (ByteBuffer) result.getEntity().get(VECTOR_FIELD);
                System.out.println(Arrays.toString(vector.array()));
            }

            SearchResp.SearchResult firstResult = results.get(0);
            if ((long)firstResult.getId() != k) {
                throw new RuntimeException(String.format("The top1 ID %d is not equal to target vector's ID %d",
                        (long)firstResult.getId(), k));
            }
        }
        System.out.println("Search result is correct");

        // Retrieve some data
        int n = 99;
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(String.format("id == %d", n))
                .outputFields(Collections.singletonList(VECTOR_FIELD))
                .build());

        List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
        if (queryResults.isEmpty()) {
            throw new RuntimeException("The query result is empty");
        } else {
            ByteBuffer vector = (ByteBuffer) queryResults.get(0).getEntity().get(VECTOR_FIELD);
            if (vector.compareTo(vectors.get(n)) != 0) {
                throw new RuntimeException("The query result is incorrect");
            }
        }
        System.out.println("Query result is correct");


        // Drop the collection if you don't need the collection anymore
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        client.close();
    }
}

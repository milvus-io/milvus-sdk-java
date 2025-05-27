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
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class TextMatchExample {
    private static final String COLLECTION_NAME = "java_sdk_example_text_match_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithFilter(MilvusClientV2 client, String filter) {
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(filter)
                .outputFields(Collections.singletonList("text"))
                .build());
        System.out.println("\nQuery with filter: " + filter);
        List<QueryResp.QueryResult> records = queryRet.getQueryResults();
        for (QueryResp.QueryResult record : records) {
            System.out.println(record);
        }
        System.out.printf("%d items matched%n", records.size());
        System.out.println("=============================================================");
    }

    private static void searchWithFilter(MilvusClientV2 client, String filter) {
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(new FloatVec(CommonUtils.generateFloatVector(VECTOR_DIM))))
                .filter(filter)
                .topK(10)
                .outputFields(Collections.singletonList("text"))
                .build());
        System.out.println("\nSearch by filter: " + filter);
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("ID: %d, Score: %f, %s\n", (long)result.getId(), result.getScore(), result.getEntity().toString());
            }
        }
        System.out.println("=============================================================");
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
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("type", "english");
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(1000)
                .enableAnalyzer(true)
                .analyzerParams(analyzerParams)
                .enableMatch(true) // must enable this if you use TextMatch
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert rows
        Gson gson = new Gson();
        List<JsonObject> rows = Arrays.asList(
                gson.fromJson("{\"id\": 0, \"text\": \"Milvus is an open-source vector database\"}", JsonObject.class),
                gson.fromJson("{\"id\": 1, \"text\": \"AI applications help people better life\"}", JsonObject.class),
                gson.fromJson("{\"id\": 2, \"text\": \"Will the electric car replace gas-powered car?\"}", JsonObject.class),
                gson.fromJson("{\"id\": 3, \"text\": \"LangChain is a composable framework to build with LLMs. Milvus is integrated into LangChain.\"}", JsonObject.class),
                gson.fromJson("{\"id\": 4, \"text\": \"RAG is the process of optimizing the output of a large language model\"}", JsonObject.class),
                gson.fromJson("{\"id\": 5, \"text\": \"Newton is one of the greatest scientist of human history\"}", JsonObject.class),
                gson.fromJson("{\"id\": 6, \"text\": \"Metric type L2 is Euclidean distance\"}", JsonObject.class),
                gson.fromJson("{\"id\": 7, \"text\": \"Embeddings represent real-world objects, like words, images, or videos, in a form that computers can process.\"}", JsonObject.class),
                gson.fromJson("{\"id\": 8, \"text\": \"The moon is 384,400 km distance away from earth\"}", JsonObject.class),
                gson.fromJson("{\"id\": 9, \"text\": \"Milvus supports L2 distance and IP similarity for float vector.\"}", JsonObject.class)
        );

        // TextMatch is keyword filtering, here we just fill the vector field by random vectors
        for (JsonObject obj : rows) {
            obj.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
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
        System.out.printf("%d rows in collection\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // TEXT_MATCH requires the data is persisted
        client.flush(FlushReq.builder().collectionNames(Collections.singletonList(COLLECTION_NAME)).build());

        // Query by keyword filtering expression
        queryWithFilter(client, "TEXT_MATCH(text, \"distance\")");
        queryWithFilter(client, "TEXT_MATCH(text, \"Milvus\") or TEXT_MATCH(text, \"distance\")");
        queryWithFilter(client, "TEXT_MATCH(text, \"Euclidean\") and TEXT_MATCH(text, \"distance\")");

        // Search by keyword filtering expression
        searchWithFilter(client, "TEXT_MATCH(text, \"distance\")");
        searchWithFilter(client, "TEXT_MATCH(text, \"Euclidean distance\")");
        searchWithFilter(client, "TEXT_MATCH(text, \"vector database\")");

        client.close();
    }
}

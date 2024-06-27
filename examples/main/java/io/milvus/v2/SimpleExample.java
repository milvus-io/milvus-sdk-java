package io.milvus.v2;

import com.google.gson.*;
import io.milvus.v2.client.*;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.*;

import java.util.*;

public class SimpleExample {
    public static void main(String[] args) {

        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        String collectionName = "java_sdk_example_simple_v2";
        // drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .build());

        // quickly create a collection with "id" field and "vector" field
        client.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .dimension(4)
                .build());
        System.out.printf("Collection '%s' created\n", collectionName);

        // insert some data
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.add("vector", gson.toJsonTree(new float[]{i, (float) i /2, (float) i /3, (float) i /4}));
            row.addProperty(String.format("dynamic_%d", i), "this is dynamic value"); // this value is stored in dynamic field
            rows.add(row);
        }
        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName(collectionName)
                .data(rows)
                .build());
        System.out.printf("%d rows inserted\n", insertR.getInsertCnt());

        // get row count
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName(collectionName)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // retrieve
        List<Object> ids = Arrays.asList(1L, 50L);
        GetResp getR = client.get(GetReq.builder()
                .collectionName(collectionName)
                .ids(ids)
                .outputFields(Collections.singletonList("*"))
                .build());
        System.out.println("\nRetrieve results:");
        for (QueryResp.QueryResult result : getR.getGetResults()) {
            System.out.println(result.getEntity());
        }

        // search
        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName(collectionName)
                .data(Collections.singletonList(new FloatVec(new float[]{1.0f, 1.0f, 1.0f, 1.0f})))
                .filter("id < 100")
                .topK(10)
                .outputFields(Collections.singletonList("*"))
                .build());
        List<List<SearchResp.SearchResult>> searchResults = searchR.getSearchResults();
        System.out.println("\nSearch results:");
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("ID: %d, Score: %f, %s\n", (long)result.getId(), result.getScore(), result.getEntity().toString());
            }
        }
    }
}

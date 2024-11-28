package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.common.utils.JsonUtils;
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
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;

public class ArrayFieldExample {
    private static final String COLLECTION_NAME = "java_sdk_example_array_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClientV2 client, String expr) {
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(expr)
                .outputFields(Arrays.asList("array_int32", "array_varchar"))
                .build());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResp.QueryResult> records = queryRet.getQueryResults();
        for (QueryResp.QueryResult record : records) {
            System.out.println(record.getEntity());
        }
        System.out.printf("%d items matched%n", records.size());
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
                .autoID(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("array_int32")
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .maxCapacity(10)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("array_varchar")
                .dataType(DataType.Array)
                .elementType(DataType.VarChar)
                .maxCapacity(10)
                .maxLength(100)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName(VECTOR_FIELD)
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
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
        Random random = new Random();
        int rowCount = 100;
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            JsonObject row = new JsonObject();
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            List<Integer> intArray = new ArrayList<>();
            List<String> strArray = new ArrayList<>();
            int capacity = random.nextInt(5) + 5;
            for (int k = 0; k < capacity; k++) {
                intArray.add((i+k)%100);
                strArray.add(String.format("string-%d-%d", i, k));
            }
            row.add("array_int32", JsonUtils.toJsonTree(intArray).getAsJsonArray());
            row.add("array_varchar", JsonUtils.toJsonTree(strArray).getAsJsonArray());
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
        System.out.printf("%d rows in collection\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));

        // Query by filtering expression
        queryWithExpr(client, "array_int32[0] == 99");
        queryWithExpr(client, "array_int32[1] in [5, 10, 15]");
        queryWithExpr(client, "array_varchar[0] like \"string-55%\"");
        queryWithExpr(client, "array_contains(array_varchar, \"string-4-1\")");
        queryWithExpr(client, "array_contains_any(array_int32, [3, 9])");
        queryWithExpr(client, "array_contains_all(array_int32, [3, 9])");

        client.close();
    }
}

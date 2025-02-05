package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonNull;
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
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;

public class NullAndDefaultExample {
    private static final String COLLECTION_NAME = "java_sdk_example_nullable_v2";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClientV2 client, String expr) {
        QueryResp queryRet = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(expr)
                .outputFields(Arrays.asList("nullable_test", "default_test", "nullable_default"))
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
                .autoID(false)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName(VECTOR_FIELD)
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("nullable_test")
                .dataType(DataType.Int32)
                .isNullable(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("default_test")
                .dataType(DataType.Double)
                .defaultValue(3.1415)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("nullable_default")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .isNullable(true)
                .defaultValue("I am default value")
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
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            // half of values are null
            if ((i % 2 == 0)) {
                row.addProperty("nullable_test", i);
            } else {
                row.add("nullable_test", JsonNull.INSTANCE);
            }

            // some values are default value
            if (i%3==0) {
                row.addProperty("default_test", 1.0);
            }

            // some values are null, some values are default value
            if (i > 5) {
                if ((i % 2 == 0)) {
                    row.addProperty("nullable_default", String.format("val_%d", i));
                } else {
                    // if default value is set, null value will be replaced by default value
                    row.add("nullable_default", JsonNull.INSTANCE);
                }
            }

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
        queryWithExpr(client, "id >= 0"); // show all items
        queryWithExpr(client, "nullable_test >= 0");
        queryWithExpr(client, "default_test == 3.1415");
        queryWithExpr(client, "nullable_default != \"I am default value\"");

        client.close();
    }
}

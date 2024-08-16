package io.milvus.v1;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.QueryResults;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.QueryResultsWrapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JsonFieldExample {
    private static final String COLLECTION_NAME = "java_sdk_example_json_v1";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final String JSON_FIELD = "metadata";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClient client, String expr) {
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr)
                .addOutField(ID_FIELD)
                .addOutField(JSON_FIELD)
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResultsWrapper.RowRecord> records = queryWrapper.getRowRecords();
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
        }
        System.out.println("=============================================================");
    }

    public static void main(String[] args) {
        // Connect to Milvus server. Replace the "localhost" and port with your Milvus server address.
        MilvusServiceClient client = new MilvusServiceClient(ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build());

        // Define fields
        List<FieldType> fieldsSchema = Arrays.asList(
                FieldType.newBuilder()
                        .withName(ID_FIELD)
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build(),
                FieldType.newBuilder()
                        .withName(VECTOR_FIELD)
                        .withDataType(DataType.FloatVector)
                        .withDimension(VECTOR_DIM)
                        .build(),
                FieldType.newBuilder()
                        .withName(JSON_FIELD)
                        .withDataType(DataType.JSON)
                        .build()
        );

        CollectionSchemaParam collectionSchemaParam = CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(false)
                .withFieldTypes(fieldsSchema)
                .build();

        // Drop the collection if exists
        client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        // Create the collection with 3 fields
        R<RpcStatus> ret = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withSchema(collectionSchemaParam)
                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage());
        }

        // Specify an index type on the vector field.
        ret = client.createIndex(CreateIndexParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldName(VECTOR_FIELD)
                .withIndexType(IndexType.FLAT)
                .withMetricType(MetricType.L2)
                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create index on vector field! Error: " + ret.getMessage());
        }

        // Call loadCollection() to enable automatically loading data into memory for searching
        client.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        System.out.println("Collection created");

        // insert rows
        Gson gson = new Gson();
        for (int i = 0; i < 100; i++) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));

            JsonObject metadata = new JsonObject();
            metadata.addProperty("path", String.format("\\root/abc/path%d", i));
            metadata.addProperty("size", i);
            if (i%7 == 0) {
                metadata.addProperty("special", true);
            }
            metadata.add("flags", gson.toJsonTree(Arrays.asList(i, i + 1, i + 2)));
            row.add(JSON_FIELD, metadata);
            System.out.println(metadata);

            client.insert(InsertParam.newBuilder()
                    .withCollectionName(COLLECTION_NAME)
                    .withRows(Collections.singletonList(row))
                    .build());
        }

        // get row count
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr("")
                .addOutField("count(*)")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        long rowCount = (long)queryWrapper.getFieldWrapper("count(*)").getFieldData().get(0);
        System.out.printf("%d rows persisted\n", rowCount);

        // query by filtering JSON
        queryWithExpr(client, "exists metadata[\"special\"]");
        queryWithExpr(client, "metadata[\"size\"] < 5");
        queryWithExpr(client, "metadata[\"size\"] in [4, 5, 6]");
        queryWithExpr(client, "JSON_CONTAINS(metadata[\"flags\"], 9)");
        queryWithExpr(client, "JSON_CONTAINS_ANY(metadata[\"flags\"], [8, 9, 10])");
        queryWithExpr(client, "JSON_CONTAINS_ALL(metadata[\"flags\"], [8, 9, 10])");
    }
}

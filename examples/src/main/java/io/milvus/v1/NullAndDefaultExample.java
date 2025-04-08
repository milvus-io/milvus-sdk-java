package io.milvus.v1;

import com.google.gson.Gson;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.QueryResults;
import io.milvus.param.*;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.QueryResultsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NullAndDefaultExample {
    private static final String COLLECTION_NAME = "java_sdk_example_nullable_v1";
    private static final String ID_FIELD = "id";
    private static final String VECTOR_FIELD = "vector";
    private static final Integer VECTOR_DIM = 128;

    private static void queryWithExpr(MilvusClient client, String expr) {
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr(expr)
                .addOutField("nullable_test")
                .addOutField("default_test")
                .addOutField("nullable_default")
                .addOutField("nullable_array")
                .build());
        QueryResultsWrapper queryWrapper = new QueryResultsWrapper(queryRet.getData());
        System.out.println("\nQuery with expression: " + expr);
        List<QueryResultsWrapper.RowRecord> records = queryWrapper.getRowRecords();
        for (QueryResultsWrapper.RowRecord record : records) {
            System.out.println(record);
        }
        System.out.printf("%d items matched%n", records.size());
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
                        .withName("nullable_test")
                        .withDataType(DataType.Int64)
                        .withNullable(true)
                        .build(),
                FieldType.newBuilder()
                        .withName("default_test")
                        .withDataType(DataType.Double)
                        .withDefaultValue(3.1415)
                        .build(),
                FieldType.newBuilder()
                        .withName("nullable_default")
                        .withDataType(DataType.VarChar)
                        .withMaxLength(64)
                        .withDefaultValue("I am default value")
                        .withNullable(true)
                        .build(),
                FieldType.newBuilder()
                        .withName("nullable_array")
                        .withDataType(DataType.Array)
                        .withElementType(DataType.VarChar)
                        .withMaxCapacity(10)
                        .withMaxLength(100)
                        .withNullable(true)
                        .build()
        );

        // Drop the collection if exists
        client.dropCollection(DropCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .build());

        // Create the collection
        R<RpcStatus> ret = client.createCollection(CreateCollectionParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withFieldTypes(fieldsSchema)
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

        // Insert 10 records into the collection
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            JsonObject row = new JsonObject();
            row.addProperty(ID_FIELD, i);
            row.add(VECTOR_FIELD, gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            // half of values are null
            if ((i % 2 == 0)) {
                row.addProperty("nullable_test", i);
            } else {
                row.add("nullable_test", JsonNull.INSTANCE);

                List<String> arr = Arrays.asList("A", "B", "C");
                row.add("nullable_array", gson.toJsonTree(arr));
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

        R<MutationResult> insertRet = client.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        if (insertRet.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to insert! Error: " + insertRet.getMessage());
        }

        // Get row count
        R<QueryResults> queryRet = client.query(QueryParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withExpr("")
                .addOutField("count(*)")
                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                .build());
        QueryResultsWrapper wrapper = new QueryResultsWrapper(queryRet.getData());
        long rowCount = (long)wrapper.getFieldWrapper("count(*)").getFieldData().get(0);
        System.out.printf("%d rows in collection\n", rowCount);

        // Query by filtering expression
        queryWithExpr(client, "id >= 0"); // show all items
        queryWithExpr(client, "nullable_test >= 0");
        queryWithExpr(client, "default_test == 3.1415");
        queryWithExpr(client, "nullable_default != \"I am default value\"");
    }
}

package io.milvus.v2;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.milvus.bulkwriter.storage.client.MinioStorageClient;
import io.milvus.v1.BulkWriterExample;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.BulkInsertState;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.BulkInsertReq;
import io.milvus.v2.service.utility.request.GetBulkInsertStateReq;
import io.milvus.v2.service.utility.request.ListBulkInsertTasksReq;
import io.milvus.v2.service.utility.response.BulkInsertResp;
import io.milvus.v2.service.utility.response.GetBulkInsertStateResp;
import io.milvus.v2.service.utility.response.ListBulkInsertTasksResp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class BulkInsertExample {
    private static final String COLLECTION_NAME = "java_sdk_example_bulkinsert_v2";
    private static final int VECTOR_DIM = 256;

    private static final String DATA_FILE_PATH = "/tmp/milvus_java_sdk_import_test.json";

    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    // a collection with all kinds of scalars and vectors
    private static CreateCollectionReq.CollectionSchema createSchema() {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("bool_field")
                .dataType(DataType.Bool)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int8_field")
                .dataType(DataType.Int8)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int16_field")
                .dataType(DataType.Int16)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int32_field")
                .dataType(DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int64_field")
                .dataType(DataType.Int64)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_field")
                .dataType(DataType.Float)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("double_field")
                .dataType(DataType.Double)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("varchar_field")
                .dataType(DataType.VarChar)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("json_field")
                .dataType(DataType.JSON)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_int_field")
                .dataType(DataType.Array)
                .maxCapacity(50)
                .elementType(DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_float_field")
                .dataType(DataType.Array)
                .maxCapacity(20)
                .elementType(DataType.Float)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_varchar_field")
                .dataType(DataType.Array)
                .maxCapacity(10)
                .elementType(DataType.VarChar)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float16_vector")
                .dataType(DataType.Float16Vector)
                .dimension(VECTOR_DIM)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .build());

        return collectionSchema;
    }

    private static void createCollection() {
        CreateCollectionReq.CollectionSchema schema = createSchema();

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());

        indexParams.add(IndexParam.builder()
                .fieldName("binary_vector")
                .indexType(IndexParam.IndexType.BIN_FLAT)
                .metricType(IndexParam.MetricType.JACCARD)
                .build());

        indexParams.add(IndexParam.builder()
                .fieldName("float16_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("drop_ratio_build",0.2);
        indexParams.add(IndexParam.builder()
                .fieldName("sparse_vector")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(extraParams)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);
        System.out.println(String.format("Collection '%s' created", COLLECTION_NAME));
    }

    private static void prepareDataFile() {
        // prepare data
        Gson gson = new Gson();
        JsonArray data = new JsonArray();
        int rowCount = 100;
        for (int i = 0; i < rowCount; i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", i);
            row.addProperty("bool_field", Boolean.valueOf(i%3 == 0));
            row.addProperty("int8_field", i%256);
            row.addProperty("int16_field", i%65535);
            row.addProperty("int32_field", i);
            row.addProperty("int64_field", i);
            row.addProperty("float_field", i/8);
            row.addProperty("double_field", i/7);
            row.addProperty("varchar_field", String.format("varchar_%d", i));

            JsonObject jsonField = new JsonObject();
            jsonField.addProperty("flag", i);
            row.add("json_field", jsonField);

            List<Integer> intArr = new ArrayList<>();
            List<Float> floatArr = new ArrayList<>();
            List<String> varcharArr = new ArrayList<>();
            for (int k = 1; k < i%10 + 2; k++) {
                intArr.add(k);
                floatArr.add((float)k/5);
                varcharArr.add(String.format("arr_%d", k));
            }
            row.add("arr_int_field", gson.toJsonTree(intArr));
            row.add("arr_float_field", gson.toJsonTree(floatArr));
            row.add("arr_varchar_field", gson.toJsonTree(varcharArr));

            row.add("float_vector", gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM)));
            row.add("binary_vector", gson.toJsonTree(CommonUtils.generateBinaryVector(VECTOR_DIM).array()));
            row.add("float16_vector", gson.toJsonTree(CommonUtils.generateFloatVector(VECTOR_DIM))); // hopefully the float vector is converted to float16
            row.add("sparse_vector", gson.toJsonTree(CommonUtils.generateSparseVector()));

            data.add(gson.toJsonTree(row));
        }

        // generate a JSON file
        try (FileWriter writer = new FileWriter(DATA_FILE_PATH)) {
            writer.write(gson.toJson(data));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        System.out.println("JSON file generated: " + DATA_FILE_PATH);

        // upload the JSON file
        MinioStorageClient storageClient = MinioStorageClient.getStorageClient(
                BulkWriterExample.StorageConsts.cloudStorage.getCloudName(),
                BulkWriterExample.StorageConsts.STORAGE_ENDPOINT,
                BulkWriterExample.StorageConsts.STORAGE_ACCESS_KEY,
                BulkWriterExample.StorageConsts.STORAGE_SECRET_KEY,
                "",
                BulkWriterExample.StorageConsts.STORAGE_REGION,
                null);

        try {
            File file = new File(DATA_FILE_PATH);
            FileInputStream fileInputStream = new FileInputStream(file);
            storageClient.putObjectStream(fileInputStream, file.length(), BulkWriterExample.StorageConsts.STORAGE_BUCKET, DATA_FILE_PATH);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println("JSON file uploaded");
    }

    private static void doBulkInsert() {
        // import the JSON file
        BulkInsertResp importResp = client.bulkInsert(BulkInsertReq.builder()
                .collectionName(COLLECTION_NAME)
                .files(Collections.singletonList(DATA_FILE_PATH))
                .build());
        List<Long> taskIDs = importResp.getTasks();
        System.out.println(taskIDs.get(0));
        System.out.println("JSON file imported");

        // list import tasks
        ListBulkInsertTasksResp listResp = client.listBulkInsertTasks(ListBulkInsertTasksReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        List<GetBulkInsertStateResp> tasks = listResp.getTasks();
        if (tasks.isEmpty()) {
            System.out.println("Not able to list import tasks");
            return;
        }

        // get import task state
        while (true) {
            try {
                System.out.println("Wait 1 second to check bulkinsert task state");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("Thread was interrupted");
                return;
            }

            GetBulkInsertStateResp getResp = client.getBulkInsertState(GetBulkInsertStateReq.builder()
                    .taskID(taskIDs.get(0))
                    .build());

            if (getResp.getState() == BulkInsertState.ImportFailed ||
                    getResp.getState() == BulkInsertState.ImportFailedAndCleaned ||
                    getResp.getState() == BulkInsertState.ImportCompleted) {
                System.out.println(getResp);
                System.out.println("Import done");
                break;
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        createCollection();
        prepareDataFile();
        doBulkInsert();
        client.close(5);
        System.out.println("Milvus client disconnected");
    }
}

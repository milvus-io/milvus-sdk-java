/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.milvus.v2.bulkwriter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.milvus.bulkwriter.RemoteBulkWriter;
import io.milvus.bulkwriter.RemoteBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.clientenum.CloudStorage;
import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.bulkwriter.request.describe.MilvusDescribeImportRequest;
import io.milvus.bulkwriter.request.import_.MilvusImportRequest;
import io.milvus.bulkwriter.restful.BulkImportUtils;
import io.milvus.v1.CommonUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.RefreshLoadReq;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.response.ListDatabasesResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BulkWriterStructExample {
    public static final String HOST = "127.0.0.1";
    public static final Integer PORT = 19530;
    public static final String USER_NAME = "user.name";
    public static final String PASSWORD = "password";

    public static class StorageConsts {
        public static final CloudStorage cloudStorage = CloudStorage.MINIO;
        public static final String STORAGE_ENDPOINT = cloudStorage.getEndpoint("http://127.0.0.1:9000");
        public static final String STORAGE_BUCKET = "a-bucket";
        public static final String STORAGE_ACCESS_KEY = "minioadmin";
        public static final String STORAGE_SECRET_KEY = "minioadmin";
        public static final String STORAGE_REGION = "";
        public static final String AZURE_CONTAINER_NAME = "azure.container.name";
        public static final String AZURE_ACCOUNT_NAME = "azure.account.name";
        public static final String AZURE_ACCOUNT_KEY = "azure.account.key";
    }

    private static final Gson GSON = new Gson();
    private static final String DATABASE_NAME = "java_sdk_db";
    private static final String COLLECTION_NAME_PREFIX = "java_sdk_bulkwriter_struct_v2";
    private static final String STRUCT_FIELD = "struct_field";
    private static final String FLOAT_VECTOR_FIELD = "st_float_vector";
    private static final String BINARY_VECTOR_FIELD = "st_binary_vector";
    private static final String FLOAT16_VECTOR_FIELD = "st_float16_vector";
    private static final String BFLOAT16_VECTOR_FIELD = "st_bfloat16_vector";
    private static final String INT8_VECTOR_FIELD = "st_int8_vector";
    private static final String NAME_FIELD = "st_name";
    private static final int DIM = 4;
    private static final int BIN_DIM = 16;
    private static final int IVF_NLIST = 64;
    private static final List<BulkFileType> FILE_TYPES = Arrays.asList(BulkFileType.PARQUET, BulkFileType.JSON, BulkFileType.CSV);
    private static MilvusClientV2 milvusClient;

    public static void main(String[] args) throws Exception {
        try {
            createConnection();
            CreateCollectionReq.CollectionSchema schema = buildSchema();
            List<Map<String, Object>> originalRows = buildOriginalRows(5);
            List<JsonObject> importRows = buildImportRows(originalRows);
            for (BulkFileType fileType : FILE_TYPES) {
                System.out.println("\n=================================================================================");
                String collectionName = String.format("%s_%s", COLLECTION_NAME_PREFIX, fileType.name().toLowerCase());
                System.out.printf("Running struct vector import with file type %s on collection %s%n", fileType, collectionName);
                createCollection(collectionName, schema);
                List<List<String>> batchFiles = writeRows(collectionName, schema, importRows, fileType);
                callBulkInsert(collectionName, batchFiles);
                verifyImport(collectionName, originalRows);
            }
        } finally {
            if (milvusClient != null) {
                milvusClient.close();
            }
        }
    }

    private static void createConnection() {
        String url = String.format("http://%s:%s", HOST, PORT);
        milvusClient = new MilvusClientV2(ConnectConfig.builder()
                .uri(url)
                .username(USER_NAME)
                .password(PASSWORD)
                .build());

        ListDatabasesResp dbs = milvusClient.listDatabases();
        if (!dbs.getDatabaseNames().contains(DATABASE_NAME)) {
            milvusClient.createDatabase(CreateDatabaseReq.builder()
                    .databaseName(DATABASE_NAME)
                    .build());
        }
        try {
            milvusClient.useDatabase(DATABASE_NAME);
        } catch (Exception e) {
            System.out.println("Unable to switch database, error: " + e);
        }
    }

    private static CreateCollectionReq.CollectionSchema buildSchema() {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName(STRUCT_FIELD)
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(8)
                .addStructField(AddFieldReq.builder()
                        .fieldName(NAME_FIELD)
                        .dataType(DataType.VarChar)
                        .maxLength(128)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(FLOAT_VECTOR_FIELD)
                        .dataType(DataType.FloatVector)
                        .dimension(DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(BINARY_VECTOR_FIELD)
                        .dataType(DataType.BinaryVector)
                        .dimension(BIN_DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(FLOAT16_VECTOR_FIELD)
                        .dataType(DataType.Float16Vector)
                        .dimension(DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(BFLOAT16_VECTOR_FIELD)
                        .dataType(DataType.BFloat16Vector)
                        .dimension(DIM)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName(INT8_VECTOR_FIELD)
                        .dataType(DataType.Int8Vector)
                        .dimension(DIM)
                        .build())
                .build());
        return schema;
    }

    private static List<Map<String, Object>> buildOriginalRows(int rowCount) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("id", (long) i);

            List<Map<String, Object>> structList = new ArrayList<>();
            for (int j = 0; j < i % 3 + 1; j++) {
                structList.add(buildOriginalStruct(i, j));
            }
            row.put(STRUCT_FIELD, structList);
            rows.add(row);
        }
        return rows;
    }

    private static Map<String, Object> buildOriginalStruct(int rowId, int structId) {
        Map<String, Object> struct = new HashMap<>();
        struct.put(NAME_FIELD, String.format("row_%d_struct_%d", rowId, structId));

        List<Float> floatVector = CommonUtils.generateFloatVector(DIM);
        List<Float> float16SourceVector = CommonUtils.generateFloatVector(DIM);
        List<Float> bfloat16SourceVector = CommonUtils.generateFloatVector(DIM);
        struct.put(FLOAT_VECTOR_FIELD, floatVector);
        struct.put(BINARY_VECTOR_FIELD, CommonUtils.generateBinaryVector(BIN_DIM));
        struct.put(FLOAT16_VECTOR_FIELD, CommonUtils.encodeFloat16Vector(float16SourceVector, false));
        struct.put(BFLOAT16_VECTOR_FIELD, CommonUtils.encodeFloat16Vector(bfloat16SourceVector, true));
        struct.put(INT8_VECTOR_FIELD, CommonUtils.generateInt8Vector(DIM));
        return struct;
    }

    private static List<JsonObject> buildImportRows(List<Map<String, Object>> originalRows) {
        List<JsonObject> rows = new ArrayList<>();
        for (Map<String, Object> originalRow : originalRows) {
            JsonObject row = new JsonObject();
            row.addProperty("id", (Number) originalRow.get("id"));

            JsonArray structArray = new JsonArray();
            List<Map<String, Object>> structs = (List<Map<String, Object>>) originalRow.get(STRUCT_FIELD);
            for (Map<String, Object> originalStruct : structs) {
                structArray.add(buildImportStruct(originalStruct));
            }
            row.add(STRUCT_FIELD, structArray);
            rows.add(row);
        }
        return rows;
    }

    private static JsonObject buildImportStruct(Map<String, Object> originalStruct) {
        JsonObject struct = new JsonObject();
        struct.addProperty(NAME_FIELD, (String) originalStruct.get(NAME_FIELD));
        struct.add(FLOAT_VECTOR_FIELD, GSON.toJsonTree(originalStruct.get(FLOAT_VECTOR_FIELD)));
        struct.add(BINARY_VECTOR_FIELD, GSON.toJsonTree(((ByteBuffer) originalStruct.get(BINARY_VECTOR_FIELD)).array()));
        struct.add(FLOAT16_VECTOR_FIELD, GSON.toJsonTree(((ByteBuffer) originalStruct.get(FLOAT16_VECTOR_FIELD)).array()));
        struct.add(BFLOAT16_VECTOR_FIELD, GSON.toJsonTree(((ByteBuffer) originalStruct.get(BFLOAT16_VECTOR_FIELD)).array()));
        struct.add(INT8_VECTOR_FIELD, GSON.toJsonTree(((ByteBuffer) originalStruct.get(INT8_VECTOR_FIELD)).array()));
        return struct;
    }

    private static List<List<String>> writeRows(String collectionName, CreateCollectionReq.CollectionSchema schema, List<JsonObject> rows, BulkFileType fileType) throws Exception {
        try (RemoteBulkWriter remoteBulkWriter = buildRemoteBulkWriter(collectionName, schema, fileType)) {
            for (JsonObject row : rows) {
                remoteBulkWriter.appendRow(row);
            }
            System.out.printf("%s rows appended for %s%n", remoteBulkWriter.getTotalRowCount(), fileType);
            remoteBulkWriter.commit(false);
            System.out.printf("Uploaded files for %s: %s%n", fileType, remoteBulkWriter.getBatchFiles());
            return remoteBulkWriter.getBatchFiles();
        }
    }

    private static RemoteBulkWriter buildRemoteBulkWriter(String collectionName, CreateCollectionReq.CollectionSchema schema, BulkFileType fileType) throws IOException {
        RemoteBulkWriterParam bulkWriterParam = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(schema)
                .withRemotePath(String.format("bulk_data/struct_vector_example/%s/%s", collectionName, fileType.name().toLowerCase()))
                .withFileType(fileType)
                .withChunkSize(128 * 1024 * 1024)
                .withConnectParam(buildStorageConnectParam())
                .build();
        return new RemoteBulkWriter(bulkWriterParam);
    }

    private static StorageConnectParam buildStorageConnectParam() {
        if (CloudStorage.isAzCloud(StorageConsts.cloudStorage.getCloudName())) {
            String connectionStr = "DefaultEndpointsProtocol=https;AccountName=" + StorageConsts.AZURE_ACCOUNT_NAME
                    + ";AccountKey=" + StorageConsts.AZURE_ACCOUNT_KEY
                    + ";EndpointSuffix=core.windows.net";
            return AzureConnectParam.newBuilder()
                    .withConnStr(connectionStr)
                    .withContainerName(StorageConsts.AZURE_CONTAINER_NAME)
                    .build();
        }

        return S3ConnectParam.newBuilder()
                .withEndpoint(StorageConsts.STORAGE_ENDPOINT)
                .withCloudName(StorageConsts.cloudStorage.getCloudName())
                .withBucketName(StorageConsts.STORAGE_BUCKET)
                .withAccessKey(StorageConsts.STORAGE_ACCESS_KEY)
                .withSecretKey(StorageConsts.STORAGE_SECRET_KEY)
                .withRegion(StorageConsts.STORAGE_REGION)
                .build();
    }

    private static void createCollection(String collectionName, CreateCollectionReq.CollectionSchema schema) {
        milvusClient.dropCollection(DropCollectionReq.builder()
                .collectionName(collectionName)
                .databaseName(DATABASE_NAME)
                .build());
        milvusClient.createCollection(CreateCollectionReq.builder()
                .collectionName(collectionName)
                .databaseName(DATABASE_NAME)
                .collectionSchema(schema)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build());
        System.out.printf("Collection %s created%n", collectionName);
    }

    private static void callBulkInsert(String collectionName, List<List<String>> batchFiles) throws InterruptedException {
        String url = String.format("http://%s:%s", HOST, PORT);
        MilvusImportRequest request = MilvusImportRequest.builder()
                .collectionName(collectionName)
                .dbName(DATABASE_NAME)
                .files(batchFiles)
                .apiKey(USER_NAME + ":" + PASSWORD)
                .build();
        String importResult = BulkImportUtils.bulkImport(url, request);
        System.out.println(importResult);

        String jobId = GSON.fromJson(importResult, JsonObject.class)
                .getAsJsonObject("data")
                .get("jobId")
                .getAsString();

        while (true) {
            TimeUnit.SECONDS.sleep(5);
            String progressResult = BulkImportUtils.getImportProgress(url, MilvusDescribeImportRequest.builder()
                    .jobId(jobId)
                    .apiKey(USER_NAME + ":" + PASSWORD)
                    .build());
            JsonObject progress = GSON.fromJson(progressResult, JsonObject.class);
            String state = progress.getAsJsonObject("data").get("state").getAsString();
            if ("Failed".equals(state)) {
                String reason = progress.getAsJsonObject("data").get("reason").getAsString();
                throw new RuntimeException(String.format("Import job %s failed: %s", jobId, reason));
            }
            if ("Completed".equals(state)) {
                System.out.printf("Import job %s completed%n", jobId);
                break;
            }
            System.out.printf("Import job %s is running, state=%s%n", jobId, state);
        }
    }

    private static void createIndexes(String collectionName) {
        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, FLOAT_VECTOR_FIELD))
                .indexName("index_st_float_vector")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.MAX_SIM_COSINE)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, BINARY_VECTOR_FIELD))
                .indexName("index_st_binary_vector")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.MAX_SIM_HAMMING)
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, FLOAT16_VECTOR_FIELD))
                .indexName("index_st_float16_vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.MAX_SIM_COSINE)
                .extraParams(buildIvfParams())
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, BFLOAT16_VECTOR_FIELD))
                .indexName("index_st_bfloat16_vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.MAX_SIM_COSINE)
                .extraParams(buildIvfParams())
                .build());
        indexParams.add(IndexParam.builder()
                .fieldName(String.format("%s[%s]", STRUCT_FIELD, INT8_VECTOR_FIELD))
                .indexName("index_st_int8_vector")
                .indexType(IndexParam.IndexType.HNSW)
                .metricType(IndexParam.MetricType.MAX_SIM_L2)
                .build());
        milvusClient.createIndex(CreateIndexReq.builder()
                .collectionName(collectionName)
                .databaseName(DATABASE_NAME)
                .indexParams(indexParams)
                .build());
    }

    private static Map<String, Object> buildIvfParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("nlist", IVF_NLIST);
        return params;
    }

    private static void verifyImport(String collectionName, List<Map<String, Object>> originalRows) {
        createIndexes(collectionName);
        milvusClient.loadCollection(LoadCollectionReq.builder()
                .collectionName(collectionName)
                .databaseName(DATABASE_NAME)
                .build());
        milvusClient.refreshLoad(RefreshLoadReq.builder()
                .collectionName(collectionName)
                .databaseName(DATABASE_NAME)
                .build());

        List<Long> ids = Arrays.asList(0L, (long) originalRows.size() - 1);
        QueryResp queryResp = milvusClient.query(QueryReq.builder()
                .collectionName(collectionName)
                .databaseName(DATABASE_NAME)
                .filter(String.format("id in %s", ids))
                .outputFields(Arrays.asList("*"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        if (results.size() != ids.size()) {
            throw new RuntimeException("Unexpected query result count");
        }

        for (QueryResp.QueryResult result : results) {
            Map<String, Object> entity = result.getEntity();
            long id = ((Number) entity.get("id")).longValue();
            compareStructList((List<Map<String, Object>>) originalRows.get((int) id).get(STRUCT_FIELD), entity.get(STRUCT_FIELD));
            System.out.printf("Verified row %d: %s%n", id, entity);
        }
        System.out.println("RemoteBulkWriter struct vector import is correct!");
    }

    private static void compareStructList(List<Map<String, Object>> expectedStructs, Object fetchedValue) {
        if (!(fetchedValue instanceof List<?>)) {
            throw new RuntimeException("struct_field should be returned as a list");
        }

        List<?> fetchedStructs = (List<?>) fetchedValue;
        if (fetchedStructs.size() != expectedStructs.size()) {
            throw new RuntimeException("Struct element count is unmatched");
        }

        for (int i = 0; i < fetchedStructs.size(); i++) {
            if (!(fetchedStructs.get(i) instanceof Map<?, ?>)) {
                throw new RuntimeException("Struct element should be returned as a map");
            }
            compareStruct(expectedStructs.get(i), (Map<?, ?>) fetchedStructs.get(i));
        }
    }

    private static void compareStruct(Map<String, Object> expectedStruct, Map<?, ?> fetchedStruct) {
        String expectedName = (String) expectedStruct.get(NAME_FIELD);
        Object fetchedName = fetchedStruct.get(NAME_FIELD);
        if (!expectedName.equals(fetchedName)) {
            throw new RuntimeException("Struct field st_name is unmatched");
        }

        compareFloatVector((List<Float>) expectedStruct.get(FLOAT_VECTOR_FIELD), fetchedStruct.get(FLOAT_VECTOR_FIELD), FLOAT_VECTOR_FIELD);
        compareByteVector((ByteBuffer) expectedStruct.get(BINARY_VECTOR_FIELD), fetchedStruct.get(BINARY_VECTOR_FIELD), BINARY_VECTOR_FIELD);
        compareByteVector((ByteBuffer) expectedStruct.get(FLOAT16_VECTOR_FIELD), fetchedStruct.get(FLOAT16_VECTOR_FIELD), FLOAT16_VECTOR_FIELD);
        compareByteVector((ByteBuffer) expectedStruct.get(BFLOAT16_VECTOR_FIELD), fetchedStruct.get(BFLOAT16_VECTOR_FIELD), BFLOAT16_VECTOR_FIELD);
        compareByteVector((ByteBuffer) expectedStruct.get(INT8_VECTOR_FIELD), fetchedStruct.get(INT8_VECTOR_FIELD), INT8_VECTOR_FIELD);
    }

    private static void compareFloatVector(List<Float> expectedVector, Object fetchedValue, String fieldName) {
        if (!(fetchedValue instanceof List<?>)) {
            throw new RuntimeException(String.format("Struct vector field '%s' should be returned as a float list", fieldName));
        }

        List<?> values = (List<?>) fetchedValue;
        List<Float> actualVector = new ArrayList<>(values.size());
        for (Object value : values) {
            actualVector.add(((Number) value).floatValue());
        }
        CommonUtils.compareFloatVectors(expectedVector, actualVector);
    }

    private static void compareByteVector(ByteBuffer expectedVector, Object fetchedValue, String fieldName) {
        if (!(fetchedValue instanceof ByteBuffer)) {
            throw new RuntimeException(String.format("Struct vector field '%s' should be returned as a ByteBuffer", fieldName));
        }

        byte[] expected = toByteArray(expectedVector);
        byte[] actual = toByteArray((ByteBuffer) fetchedValue);
        if (!Arrays.equals(expected, actual)) {
            throw new RuntimeException(String.format("Struct vector field '%s' is unmatched", fieldName));
        }
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.limit()];
        for (int i = 0; i < buffer.limit(); i++) {
            bytes[i] = buffer.get(i);
        }
        return bytes;
    }
}

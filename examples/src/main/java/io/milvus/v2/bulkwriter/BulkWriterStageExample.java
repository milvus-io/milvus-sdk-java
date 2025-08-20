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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.milvus.bulkwriter.StageBulkWriter;
import io.milvus.bulkwriter.StageBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.model.UploadFilesResult;
import io.milvus.bulkwriter.request.describe.CloudDescribeImportRequest;
import io.milvus.bulkwriter.request.import_.StageImportRequest;
import io.milvus.bulkwriter.request.list.CloudListImportJobsRequest;
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
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.RefreshLoadReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class BulkWriterStageExample {
    private static final Gson GSON_INSTANCE = new Gson();

    // milvus
    public static final String HOST = "127.0.0.1";
    public static final Integer PORT = 19530;
    public static final String USER_NAME = "user.name";
    public static final String PASSWORD = "password";

    /**
     * The value of the URL is fixed.
     * For overseas regions, it is: https://api.cloud.zilliz.com
     * For regions in China, it is: https://api.cloud.zilliz.com.cn
     */
    public static final String CLOUD_ENDPOINT = "https://api.cloud.zilliz.com";
    public static final String API_KEY = "_api_key_for_cluster_org_";


    /**
     * This is currently a private preview feature. If you need to use it, please submit a request and contact us.
     */
    public static final String STAGE_NAME = "_stage_name_for_project_";

    public static final String CLUSTER_ID = "_your_cloud_cluster_id_";
    // If db_name is not specified, use ""
    public static final String DB_NAME = "";
    public static final String COLLECTION_NAME = "_collection_name_on_the_db_";
    // If partition_name is not specified, use ""
    public static final String PARTITION_NAME = "_partition_name_on_the_collection_";

    private static final Integer DIM = 512;
    private static final Integer ARRAY_CAPACITY = 10;
    private static MilvusClientV2 milvusClient;

    public static void main(String[] args) throws Exception {
        createConnection();
        exampleCollectionRemoteStage(BulkFileType.PARQUET);
    }

    private static void createConnection() {
        System.out.println("\nCreate connection...");
        String url = String.format("http://%s:%s", HOST, PORT);
        milvusClient = new MilvusClientV2(ConnectConfig.builder()
                .uri(url)
                .username(USER_NAME)
                .password(PASSWORD)
                .build());
        System.out.println("\nConnected");
    }

    private static void exampleCollectionRemoteStage(BulkFileType fileType) throws Exception {
        List<Map<String, Object>> originalData = genOriginalData(5);
        List<JsonObject> rows = genImportData(originalData, true);

        // 4 types vectors + all scalar types + dynamic field enabled, use cloud import api.
        // You need to apply a cloud service from Zilliz Cloud(https://zilliz.com/cloud)
        CreateCollectionReq.CollectionSchema collectionSchema = buildAllTypesSchema();
        createCollection(COLLECTION_NAME, collectionSchema, false);

        UploadFilesResult stageUploadResult = stageRemoteWriter(collectionSchema, fileType, rows);
        callStageImport(stageUploadResult.getStageName(), stageUploadResult.getPath());
        verifyImportData(collectionSchema, originalData);
    }

    private static void callStageImport(String stageName, String path) throws InterruptedException {
        List<String> importDataPath = Lists.newArrayList(path);
        StageImportRequest stageImportRequest = StageImportRequest.builder()
                .apiKey(API_KEY)
                .stageName(stageName).dataPaths(Lists.newArrayList(Collections.singleton(importDataPath)))
                .clusterId(CLUSTER_ID).dbName(DB_NAME).collectionName(COLLECTION_NAME).partitionName(PARTITION_NAME)
                .build();
        String bulkImportResult = BulkImportUtils.bulkImport(CLOUD_ENDPOINT, stageImportRequest);
        System.out.println(bulkImportResult);

        JsonObject bulkImportObject = convertJsonObject(bulkImportResult);

        String jobId = bulkImportObject.getAsJsonObject("data").get("jobId").getAsString();
        System.out.println("Create a cloudImport job, job id: " + jobId);

        System.out.println("\n===================== call cloudListImportJobs ====================");
        CloudListImportJobsRequest listImportJobsRequest = CloudListImportJobsRequest.builder().clusterId(CLUSTER_ID).currentPage(1).pageSize(10).apiKey(API_KEY).build();
        String listImportJobsResult = BulkImportUtils.listImportJobs(CLOUD_ENDPOINT, listImportJobsRequest);
        System.out.println(listImportJobsResult);
        while (true) {
            System.out.println("Wait 5 second to check bulkInsert job state...");
            TimeUnit.SECONDS.sleep(5);

            System.out.println("\n===================== call cloudGetProgress ====================");
            CloudDescribeImportRequest request = CloudDescribeImportRequest.builder().clusterId(CLUSTER_ID).jobId(jobId).apiKey(API_KEY).build();
            String getImportProgressResult = BulkImportUtils.getImportProgress(CLOUD_ENDPOINT, request);
            JsonObject getImportProgressObject = convertJsonObject(getImportProgressResult);
            String importProgressState = getImportProgressObject.getAsJsonObject("data").get("state").getAsString();
            String progress = getImportProgressObject.getAsJsonObject("data").get("progress").getAsString();

            if ("Failed".equals(importProgressState)) {
                String reason = getImportProgressObject.getAsJsonObject("data").get("reason").getAsString();
                System.out.printf("The job %s failed, reason: %s%n", jobId, reason);
                break;
            } else if ("Completed".equals(importProgressState)) {
                System.out.printf("The job %s completed%n", jobId);
                break;
            } else {
                System.out.printf("The job %s is running, state:%s progress:%s%n", jobId, importProgressState, progress);
            }
        }
    }

    private static List<Map<String, Object>> genOriginalData(int count) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            Map<String, Object> row = new HashMap<>();
            // scalar field
            row.put("id", (long)i);
            row.put("bool", i % 5 == 0);
            row.put("int8", i % 128);
            row.put("int16", i % 1000);
            row.put("int32", i % 100000);
            row.put("float", (float)i / 3);
            row.put("double", (double)i / 7);
            row.put("varchar", "varchar_" + i);
            row.put("json", String.format("{\"dummy\": %s, \"ok\": \"name_%s\"}", i, i));

            // vector field
            row.put("float_vector", CommonUtils.generateFloatVector(DIM));
            row.put("binary_vector", CommonUtils.generateBinaryVector(DIM).array());
            row.put("float16_vector", CommonUtils.generateFloat16Vector(DIM, false).array());
            row.put("sparse_vector", CommonUtils.generateSparseVector());

            // array field
            row.put("array_bool", GeneratorUtils.generatorBoolValue(3));
            row.put("array_int8", GeneratorUtils.generatorInt8Value(4));
            row.put("array_int16", GeneratorUtils.generatorInt16Value(5));
            row.put("array_int32", GeneratorUtils.generatorInt32Value(6));
            row.put("array_int64", GeneratorUtils.generatorLongValue(7));
            row.put("array_varchar", GeneratorUtils.generatorVarcharValue(8, 10));
            row.put("array_float", GeneratorUtils.generatorFloatValue(9));
            row.put("array_double", GeneratorUtils.generatorDoubleValue(10));

            data.add(row);
        }
        // a special record with null/default values
        {
            Map<String, Object> row = new HashMap<>();
            // scalar field
            row.put("id", (long)data.size());
            row.put("bool", null);
            row.put("int8", null);
            row.put("int16", 16);
            row.put("int32", null);
            row.put("float", null);
            row.put("double", null);
            row.put("varchar", null);
            row.put("json", null);

            // vector field
            row.put("float_vector", CommonUtils.generateFloatVector(DIM));
            row.put("binary_vector", CommonUtils.generateBinaryVector(DIM).array());
            row.put("float16_vector", CommonUtils.generateFloat16Vector(DIM, false).array());
            row.put("sparse_vector", CommonUtils.generateSparseVector());

            // array field
            row.put("array_bool", GeneratorUtils.generatorBoolValue(10));
            row.put("array_int8", GeneratorUtils.generatorInt8Value(9));
            row.put("array_int16", null);
            row.put("array_int32", GeneratorUtils.generatorInt32Value(7));
            row.put("array_int64", GeneratorUtils.generatorLongValue(6));
            row.put("array_varchar", GeneratorUtils.generatorVarcharValue(5, 10));
            row.put("array_float", GeneratorUtils.generatorFloatValue(4));
            row.put("array_double", null);

            data.add(row);
        }
        return data;
    }

    private static List<JsonObject> genImportData(List<Map<String, Object>> originalData, boolean isEnableDynamicField) {
        List<JsonObject> data = new ArrayList<>();
        for (Map<String, Object> row : originalData) {
            JsonObject rowObject = new JsonObject();

            // scalar field
            rowObject.addProperty("id", (Number)row.get("id"));
            if (row.get("bool") != null) { // nullable value can be missed
                rowObject.addProperty("bool", (Boolean) row.get("bool"));
            }
            rowObject.addProperty("int8", row.get("int8") == null ? null : (Number) row.get("int8"));
            rowObject.addProperty("int16", row.get("int16") == null ? null : (Number) row.get("int16"));
            rowObject.addProperty("int32", row.get("int32") == null ? null : (Number) row.get("int32"));
            rowObject.addProperty("float", row.get("float") == null ? null : (Number) row.get("float"));
            if (row.get("double") != null) { // nullable value can be missed
                rowObject.addProperty("double", (Number) row.get("double"));
            }
            rowObject.addProperty("varchar", row.get("varchar") == null ? null : (String) row.get("varchar"));

            // Note: for JSON field, use gson.fromJson() to construct a real JsonObject
            // don't use rowObject.addProperty("json", jsonContent) since the value is treated as a string, not a JsonObject
            Object jsonContent = row.get("json");
            rowObject.add("json", jsonContent == null ? null : GSON_INSTANCE.fromJson((String)jsonContent, JsonElement.class));

            // vector field
            rowObject.add("float_vector", GSON_INSTANCE.toJsonTree(row.get("float_vector")));
            rowObject.add("binary_vector", GSON_INSTANCE.toJsonTree(row.get("binary_vector")));
            rowObject.add("float16_vector", GSON_INSTANCE.toJsonTree(row.get("float16_vector")));
            rowObject.add("sparse_vector", GSON_INSTANCE.toJsonTree(row.get("sparse_vector")));

            // array field
            rowObject.add("array_bool", GSON_INSTANCE.toJsonTree(row.get("array_bool")));
            rowObject.add("array_int8", GSON_INSTANCE.toJsonTree(row.get("array_int8")));
            rowObject.add("array_int16", GSON_INSTANCE.toJsonTree(row.get("array_int16")));
            rowObject.add("array_int32", GSON_INSTANCE.toJsonTree(row.get("array_int32")));
            rowObject.add("array_int64", GSON_INSTANCE.toJsonTree(row.get("array_int64")));
            rowObject.add("array_varchar", GSON_INSTANCE.toJsonTree(row.get("array_varchar")));
            rowObject.add("array_float", GSON_INSTANCE.toJsonTree(row.get("array_float")));
            rowObject.add("array_double", GSON_INSTANCE.toJsonTree(row.get("array_double")));

            // dynamic fields
            if (isEnableDynamicField) {
                rowObject.addProperty("dynamic", "dynamic_" + row.get("id"));
            }

            data.add(rowObject);
        }
        return data;
    }

    private static UploadFilesResult stageRemoteWriter(CreateCollectionReq.CollectionSchema collectionSchema,
                                                       BulkFileType fileType,
                                                       List<JsonObject> data) throws Exception {
        System.out.printf("\n===================== all field types (%s) ====================%n", fileType.name());

        try (StageBulkWriter stageBulkWriter = buildStageBulkWriter(collectionSchema, fileType)) {
            for (JsonObject rowObject : data) {
                stageBulkWriter.appendRow(rowObject);
            }
            System.out.printf("%s rows appends%n", stageBulkWriter.getTotalRowCount());
            System.out.println("Generate data files...");
            stageBulkWriter.commit(false);

            UploadFilesResult stageUploadResult = stageBulkWriter.getStageUploadResult();
            System.out.printf("Data files have been uploaded: %s%n", stageUploadResult);
            return stageUploadResult;
        } catch (Exception e) {
            System.out.println("allTypesRemoteWriter catch exception: " + e);
            throw e;
        }
    }

    private static StageBulkWriter buildStageBulkWriter(CreateCollectionReq.CollectionSchema collectionSchema, BulkFileType fileType) throws IOException {
        StageBulkWriterParam bulkWriterParam = StageBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchema)
                .withRemotePath("bulk_data")
                .withFileType(fileType)
                .withChunkSize(512 * 1024 * 1024)
                .withConfig("sep", "|") // only take effect for CSV file
                .withCloudEndpoint(CLOUD_ENDPOINT)
                .withApiKey(API_KEY)
                .withStageName(STAGE_NAME)
                .build();
        return new StageBulkWriter(bulkWriterParam);
    }

    /**
     * @param collectionSchema collection info
     * @param dropIfExist     if collection already exist, will drop firstly and then create again
     */
    private static void createCollection(String collectionName, CreateCollectionReq.CollectionSchema collectionSchema, boolean dropIfExist) {
        System.out.println("\n===================== create collection ====================");
        checkMilvusClientIfExist();

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(collectionSchema)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();

        Boolean has = milvusClient.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build());
        if (has) {
            if (dropIfExist) {
                milvusClient.dropCollection(DropCollectionReq.builder().collectionName(collectionName).build());
                milvusClient.createCollection(requestCreate);
            }
        } else {
            milvusClient.createCollection(requestCreate);
        }

        System.out.printf("Collection %s created%n", collectionName);
    }

    private static void comparePrint(CreateCollectionReq.CollectionSchema collectionSchema,
                                     Map<String, Object> expectedData, Map<String, Object> fetchedData,
                                     String fieldName) {
        CreateCollectionReq.FieldSchema field = collectionSchema.getField(fieldName);
        Object expectedValue = expectedData.get(fieldName);
        if (expectedValue == null) {
            if (field.getDefaultValue() != null) {
                expectedValue = field.getDefaultValue();
                // for Int8/Int16 value, the default value is Short type, the returned value is Integer type
                if (expectedValue instanceof Short) {
                    expectedValue = ((Short)expectedValue).intValue();
                }
            }
        }

        Object fetchedValue = fetchedData.get(fieldName);
        if (fetchedValue == null || fetchedValue instanceof JsonNull) {
            if (!field.getIsNullable()) {
                throw new RuntimeException("Field is not nullable but fetched data is null");
            }
            if (expectedValue != null) {
                throw new RuntimeException("Expected value is not null but fetched data is null");
            }
            return; // both fetchedValue and expectedValue are null
        }

        boolean matched;
        if (fetchedValue instanceof Float) {
            matched = Math.abs((Float)fetchedValue - (Float)expectedValue) < 1e-4;
        } else if (fetchedValue instanceof Double) {
            matched = Math.abs((Double)fetchedValue - (Double)expectedValue) < 1e-8;
        } else if (fetchedValue instanceof JsonElement) {
            JsonElement expectedJson = GSON_INSTANCE.fromJson((String)expectedValue, JsonElement.class);
            matched = fetchedValue.equals(expectedJson);
        } else if (fetchedValue instanceof ByteBuffer) {
            byte[] bb = ((ByteBuffer)fetchedValue).array();
            matched = Arrays.equals(bb, (byte[])expectedValue);
        } else if (fetchedValue instanceof List) {
            matched = fetchedValue.equals(expectedValue);
            // currently, for array field, null value, the server returns an empty list
            if (((List<?>) fetchedValue).isEmpty() && expectedValue==null) {
                matched = true;
            }
        } else {
            matched = fetchedValue.equals(expectedValue);
        }

        if (!matched) {
            System.out.print("Fetched value:");
            System.out.println(fetchedValue);
            System.out.print("Expected value:");
            System.out.println(expectedValue);
            throw new RuntimeException("Fetched data is unmatched");
        }
    }

    private static void verifyImportData(CreateCollectionReq.CollectionSchema collectionSchema, List<Map<String, Object>> rows) {
        createIndex();

        List<Long> QUERY_IDS = Lists.newArrayList(1L, (long)rows.get(rows.size()-1).get("id"));
        System.out.printf("Load collection and query items %s%n", QUERY_IDS);
        loadCollection();

        String expr = String.format("id in %s", QUERY_IDS);
        System.out.println(expr);

        List<QueryResp.QueryResult> results = query(expr, Lists.newArrayList("*"));
        System.out.println("Verify data...");
        if (results.size() != QUERY_IDS.size()) {
            throw new RuntimeException("Result count is incorrect");
        }
        for (QueryResp.QueryResult result : results) {
            Map<String, Object> fetchedEntity = result.getEntity();
            long id = (Long)fetchedEntity.get("id");
            Map<String, Object> originalEntity = rows.get((int)id);
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "bool");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "int8");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "int16");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "int32");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "float");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "double");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "varchar");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "json");

            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_bool");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_int8");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_int16");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_int32");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_int64");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_varchar");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_float");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "array_double");

            comparePrint(collectionSchema, originalEntity, fetchedEntity, "float_vector");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "binary_vector");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "float16_vector");
            comparePrint(collectionSchema, originalEntity, fetchedEntity, "sparse_vector");

            System.out.println(fetchedEntity);
        }
        System.out.println("Result is correct!");
    }

    private static void createIndex() {
        System.out.println("Create index...");
        checkMilvusClientIfExist();

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName("float_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("binary_vector")
                .indexType(IndexParam.IndexType.BIN_FLAT)
                .metricType(IndexParam.MetricType.HAMMING)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("float16_vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.IP)
                .build());
        indexes.add(IndexParam.builder()
                .fieldName("sparse_vector")
                .indexType(IndexParam.IndexType.SPARSE_WAND)
                .metricType(IndexParam.MetricType.IP)
                .build());

        milvusClient.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(indexes)
                .build());

        milvusClient.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
    }

    private static void loadCollection() {
        System.out.println("Refresh load collection...");
        checkMilvusClientIfExist();
        // RefreshLoad is a new interface from v2.5.3,
        // mainly used when there are new segments generated by bulkinsert request,
        // force the new segments to be loaded into memory.
        milvusClient.refreshLoad(RefreshLoadReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection row number: " + getCollectionRowCount());
    }

    private static List<QueryResp.QueryResult> query(String expr, List<String> outputFields) {
        System.out.println("========== query() ==========");
        checkMilvusClientIfExist();
        QueryReq test = QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter(expr)
                .outputFields(outputFields)
                .build();
        QueryResp response = milvusClient.query(test);
        return response.getQueryResults();
    }

    private static Long getCollectionRowCount() {
        System.out.println("========== getCollectionRowCount() ==========");
        checkMilvusClientIfExist();

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = milvusClient.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        return (long)countR.getQueryResults().get(0).getEntity().get("count(*)");
    }

    private static CreateCollectionReq.CollectionSchema buildAllTypesSchema() {
        CreateCollectionReq.CollectionSchema schemaV2 = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        // scalar field
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(false)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("bool")
                .dataType(DataType.Bool)
                .isNullable(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int8")
                .dataType(DataType.Int8)
                .defaultValue((short)88)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int16")
                .dataType(DataType.Int16)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int32")
                .dataType(DataType.Int32)
                .isNullable(true)
                .defaultValue(999999)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float")
                .dataType(DataType.Float)
                .isNullable(true)
                .defaultValue((float)3.14159)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("double")
                .dataType(DataType.Double)
                .isNullable(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("varchar")
                .dataType(DataType.VarChar)
                .maxLength(512)
                .isNullable(true)
                .defaultValue("this is default value")
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("json")
                .dataType(DataType.JSON)
                .isNullable(true)
                .build());

        // vector fields
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float_vector")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(DIM)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float16_vector")
                .dataType(DataType.Float16Vector)
                .dimension(DIM)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .build());

        // array fields
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_bool")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.Bool)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_int8")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.Int8)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_int16")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.Int16)
                .isNullable(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_int32")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.Int32)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_int64")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.Int64)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_varchar")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.VarChar)
                .maxLength(512)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_float")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.Float)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("array_double")
                .dataType(DataType.Array)
                .maxCapacity(ARRAY_CAPACITY)
                .elementType(DataType.Double)
                .isNullable(true)
                .build());

        return schemaV2;
    }

    private static void checkMilvusClientIfExist() {
        if (milvusClient == null) {
            String msg = "milvusClient is null. Please initialize it by calling createConnection() first before use.";
            throw new RuntimeException(msg);
        }
    }

    private static JsonObject convertJsonObject(String result) {
        return GSON_INSTANCE.fromJson(result, JsonObject.class);
    }
}

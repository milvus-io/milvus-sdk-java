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

package io.milvus.v2;

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
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.utility.request.GetRefreshExternalCollectionProgressReq;
import io.milvus.v2.service.utility.request.ListRefreshExternalCollectionJobsReq;
import io.milvus.v2.service.utility.request.RefreshExternalCollectionReq;
import io.milvus.v2.service.utility.response.GetRefreshExternalCollectionProgressResp;
import io.milvus.v2.service.utility.response.ListRefreshExternalCollectionJobsResp;
import io.milvus.v2.service.utility.response.RefreshExternalCollectionJobInfo;
import io.milvus.v2.service.utility.response.RefreshExternalCollectionResp;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import io.minio.*;
import io.minio.messages.Item;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Demonstrates the external collection (external table) feature:
 * <p>
 * 1. Generate a Parquet file with "id", "vector", "text" fields
 * 2. Upload the Parquet file to MinIO
 * 3. Create an external collection with externalSource pointing to the MinIO path
 * 4. Call refreshExternalCollection to tell Milvus to read the data
 * 5. Poll listRefreshExternalCollectionJobs and getRefreshExternalCollectionProgress until done
 * 6. Query and search to verify data
 * <p>
 * Prerequisites:
 * - Milvus standalone running with MinIO (default docker-compose setup)
 * - Milvus server must support external table feature
 */
public class ExternalTableExample {
    // Milvus connection
    private static final String MILVUS_URI = "http://localhost:19530";

    // MinIO connection (default Milvus standalone setup)
    private static final String MINIO_ENDPOINT = "http://127.0.0.1:9000";
    private static final String MINIO_ACCESS_KEY = "minioadmin";
    private static final String MINIO_SECRET_KEY = "minioadmin";
    private static final String MINIO_BUCKET = "a-bucket";

    // Collection config
    private static final String COLLECTION_NAME = "java_sdk_external_table_example";
    private static final String EXTERNAL_DATA_PREFIX = "external_table_example_data";
    private static final int DIM = 128;
    private static final int NUM_ROWS = 10000;

    public static void main(String[] args) throws Exception {
        // Step 1: Generate Parquet file and upload to MinIO
        generateAndUploadParquet();

        // Step 2: Create external collection
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri(MILVUS_URI)
                .build());

        String externalSource = "external_table_example_data";
        try {
            createExternalCollection(client, externalSource);

            // Step 3: Refresh external collection
            long jobId = refreshExternalCollection(client);

            // Step 4: Wait for refresh to complete
            waitForRefreshComplete(client, jobId);

            // Step 5: List all refresh jobs
            listRefreshJobs(client);

            // Step 6: Create index, load, and verify data
            createIndexAndLoad(client);
            verifyData(client);
        } finally {
            client.close();
        }
    }

    // ================================================================
    // Step 1: Generate Parquet data and upload to MinIO
    // ================================================================
    private static String generateAndUploadParquet() throws Exception {
        System.out.println("\n=== Step 1: Generate Parquet Data and Upload to MinIO ===");

        // Define Avro schema for the Parquet file
        // Fields: id (long), vector (array of float), text (string)
        String avroSchemaJson = "{"
                + "\"type\": \"record\","
                + "\"name\": \"ExternalData\","
                + "\"fields\": ["
                + "  {\"name\": \"id\", \"type\": \"long\"},"
                + "  {\"name\": \"vector\", \"type\": {\"type\": \"array\", \"items\": \"float\"}},"
                + "  {\"name\": \"text\", \"type\": \"string\"}"
                + "]"
                + "}";
        Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

        // Write Parquet file to a temp file
        // createTempFile creates an empty file; delete it so AvroParquetWriter can create it
        File tempFile = File.createTempFile("external_data_", ".parquet");
        tempFile.delete();
        tempFile.deleteOnExit();

        Configuration hadoopConf = new Configuration();
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                        new Path(tempFile.getAbsolutePath()))
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(hadoopConf)
                .build()) {

            Random random = new Random(42);
            for (int i = 0; i < NUM_ROWS; i++) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("id", (long) i);

                List<Float> vector = CommonUtils.generateFloatVector(DIM);
                record.put("vector", vector);
                record.put("text", "document_" + i);

                writer.write(record);
            }
        }
        System.out.printf("Generated Parquet file with %d rows, dim=%d%n", NUM_ROWS, DIM);

        // Upload to MinIO
        MinioClient minioClient = MinioClient.builder()
                .endpoint(MINIO_ENDPOINT)
                .credentials(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)
                .build();

        // Clean up old data
        try {
            Iterable<Result<Item>> objects = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(MINIO_BUCKET)
                    .prefix(EXTERNAL_DATA_PREFIX + "/")
                    .build());
            for (Result<Item> result : objects) {
                minioClient.removeObject(RemoveObjectArgs.builder()
                        .bucket(MINIO_BUCKET)
                        .object(result.get().objectName())
                        .build());
            }
        } catch (Exception e) {
            // Ignore if prefix doesn't exist
        }

        // Upload the Parquet file
        String objectName = EXTERNAL_DATA_PREFIX + "/data.parquet";
        byte[] fileBytes = Files.readAllBytes(tempFile.toPath());
        minioClient.putObject(PutObjectArgs.builder()
                .bucket(MINIO_BUCKET)
                .object(objectName)
                .stream(new ByteArrayInputStream(fileBytes), fileBytes.length, -1)
                .contentType("application/octet-stream")
                .build());
        System.out.printf("Uploaded to MinIO: %s/%s%n", MINIO_BUCKET, objectName);

        // externalSource is a path relative to the Milvus-configured MinIO bucket root
        String externalSource = EXTERNAL_DATA_PREFIX + "/";
        System.out.printf("External source path: %s%n", externalSource);
        return externalSource;
    }

    // ================================================================
    // Step 2: Create an external collection
    // ================================================================
    private static void createExternalCollection(MilvusClientV2 client, String externalSource) {
        System.out.println("\n=== Step 2: Create External Collection ===");

        // Build external spec as JSON
//        JsonObject externalSpec = new JsonObject();
//        externalSpec.addProperty("format", "parquet");
        JsonObject externalSpec = new JsonObject();
        externalSpec.addProperty("format", "parquet");

        // Build collection schema with external source
        // Use externalField to map collection fields to Parquet columns
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .externalSource(externalSource)
                .externalSpec(externalSpec)
                .build();

        // "product_id" in collection maps to "id" column in Parquet
        schema.addField(AddFieldReq.builder()
                .fieldName("product_id")
                .dataType(DataType.Int64)
                .externalField("id")
                .build());

        // "embedding" in collection maps to "vector" column in Parquet
        schema.addField(AddFieldReq.builder()
                .fieldName("embedding")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .externalField("vector")
                .build());

        // "content" in collection maps to "text" column in Parquet
        schema.addField(AddFieldReq.builder()
                .fieldName("content")
                .dataType(DataType.VarChar)
                .maxLength(256)
                .externalField("text")
                .build());

        // Drop if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());

        // Create the external collection
        CreateCollectionReq createReq = CreateCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .collectionSchema(schema)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(createReq);
        System.out.printf("External collection '%s' created%n", COLLECTION_NAME);

        // Verify with describe
        DescribeCollectionResp desc = client.describeCollection(
                io.milvus.v2.service.collection.request.DescribeCollectionReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .build());
        System.out.printf("  externalSource: %s%n", desc.getCollectionSchema().getExternalSource());
        System.out.printf("  externalSpec:   %s%n", desc.getCollectionSchema().getExternalSpec());
        for (CreateCollectionReq.FieldSchema field : desc.getCollectionSchema().getFieldSchemaList()) {
            if (field.getExternalField() != null && !field.getExternalField().isEmpty()) {
                System.out.printf("  field '%s' -> externalField '%s'%n",
                        field.getName(), field.getExternalField());
            }
        }
    }

    // ================================================================
    // Step 3: Refresh external collection
    // ================================================================
    private static long refreshExternalCollection(MilvusClientV2 client) {
        System.out.println("\n=== Step 3: Refresh External Collection ===");

        RefreshExternalCollectionResp resp = client.refreshExternalCollection(
                RefreshExternalCollectionReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .build());
        System.out.printf("Refresh job started, jobId: %d%n", resp.getJobId());
        return resp.getJobId();
    }

    // ================================================================
    // Step 4: Wait for refresh to complete
    // ================================================================
    private static void waitForRefreshComplete(MilvusClientV2 client, long jobId) throws InterruptedException {
        System.out.println("\n=== Step 4: Waiting for Refresh to Complete ===");

        while (true) {
            GetRefreshExternalCollectionProgressResp resp = client.getRefreshExternalCollectionProgress(
                    GetRefreshExternalCollectionProgressReq.builder()
                            .jobId(jobId)
                            .build());

            RefreshExternalCollectionJobInfo jobInfo = resp.getJobInfo();
            System.out.printf("  Job %d: state=%s, progress=%d%%%n",
                    jobInfo.getJobId(), jobInfo.getState(), jobInfo.getProgress());

            if ("RefreshCompleted".equals(jobInfo.getState())) {
                long elapsed = jobInfo.getEndTime() - jobInfo.getStartTime();
                System.out.printf("  Refresh completed in %dms%n", elapsed);
                break;
            } else if ("RefreshFailed".equals(jobInfo.getState())) {
                System.out.printf("  Refresh failed: %s%n", jobInfo.getReason());
                throw new RuntimeException("Refresh failed: " + jobInfo.getReason());
            }

            TimeUnit.SECONDS.sleep(2);
        }
    }

    // ================================================================
    // Step 5: List refresh jobs
    // ================================================================
    private static void listRefreshJobs(MilvusClientV2 client) {
        System.out.println("\n=== Step 5: List Refresh Jobs ===");

        ListRefreshExternalCollectionJobsResp resp = client.listRefreshExternalCollectionJobs(
                ListRefreshExternalCollectionJobsReq.builder()
                        .collectionName(COLLECTION_NAME)
                        .build());

        System.out.printf("Found %d refresh job(s):%n", resp.getJobs().size());
        for (RefreshExternalCollectionJobInfo job : resp.getJobs()) {
            System.out.printf("  jobId=%d, state=%s, progress=%d%%, source=%s%n",
                    job.getJobId(), job.getState(), job.getProgress(), job.getExternalSource());
        }
    }

    // ================================================================
    // Step 6: Create index, load, and verify data
    // ================================================================
    private static void createIndexAndLoad(MilvusClientV2 client) {
        System.out.println("\n=== Step 6: Create Index and Load ===");

        // Create vector index
        IndexParam indexParam = IndexParam.builder()
                .fieldName("embedding")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build();
        client.createIndex(CreateIndexReq.builder()
                .collectionName(COLLECTION_NAME)
                .indexParams(Collections.singletonList(indexParam))
                .build());
        System.out.println("Index created on 'embedding' field");

        // Load collection
        client.loadCollection(LoadCollectionReq.builder()
                .collectionName(COLLECTION_NAME)
                .build());
        System.out.println("Collection loaded");
    }

    private static void verifyData(MilvusClientV2 client) {
        System.out.println("\n=== Verify Data ===");

        // Query: get total row count
        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName(COLLECTION_NAME)
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("Total rows in collection: %s%n",
                queryResp.getQueryResults().get(0).getEntity().get("count(*)"));

        // Search: find similar vectors
        List<Float> queryVector = CommonUtils.generateFloatVector(DIM);
        ;
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName(COLLECTION_NAME)
                .data(Collections.singletonList(new FloatVec(queryVector)))
                .limit(5)
                .annsField("embedding")
                .outputFields(Collections.singletonList("content"))
                .build());
        System.out.println("Search results (top 5):");
        for (List<SearchResp.SearchResult> results : searchResp.getSearchResults()) {
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }
}

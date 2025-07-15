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

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.bulkwriter.BulkWriter;
import io.milvus.bulkwriter.LocalBulkWriter;
import io.milvus.bulkwriter.LocalBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.common.utils.ParquetReaderUtils;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import org.apache.avro.generic.GenericData;
import org.apache.http.util.Asserts;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class BulkWriterLocalExample {
    // milvus
    public static final String HOST = "127.0.0.1";
    public static final Integer PORT = 19530;
    public static final String USER_NAME = "user.name";
    public static final String PASSWORD = "password";

    private static final Gson GSON_INSTANCE = new Gson();
    private static final String SIMPLE_COLLECTION_NAME = "java_sdk_bulkwriter_simple_v2";
    private static final Integer DIM = 512;
    private static MilvusClientV2 milvusClient;

    public static void main(String[] args) throws Exception {
        createConnection();
        List<BulkFileType> fileTypes = Lists.newArrayList(
                BulkFileType.PARQUET,
                BulkFileType.JSON,
                BulkFileType.CSV
        );

        exampleSimpleCollection(fileTypes);
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

    private static void exampleSimpleCollection(List<BulkFileType> fileTypes) throws Exception {
        CreateCollectionReq.CollectionSchema collectionSchema = buildSimpleSchema();
        createCollection(SIMPLE_COLLECTION_NAME, collectionSchema, false);

        for (BulkFileType fileType : fileTypes) {
            localWriter(collectionSchema, fileType);
        }

        // parallel append
        parallelAppend(collectionSchema);
    }

    private static void localWriter(CreateCollectionReq.CollectionSchema collectionSchema, BulkFileType fileType) throws Exception {
        System.out.printf("\n===================== local writer (%s) ====================%n", fileType.name());
        LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchema)
                .withLocalPath("/tmp/bulk_writer")
                .withFileType(fileType)
                .withChunkSize(128 * 1024 * 1024)
                .build();

        try (LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam)) {
            // read data from csv
            readCsvSampleData("data/train_embeddings.csv", localBulkWriter);

            // append rows
            for (int i = 0; i < 100000; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("path", "path_" + i);
                row.add("vector", GSON_INSTANCE.toJsonTree(GeneratorUtils.genFloatVector(DIM)));
                row.addProperty("label", "label_" + i);

                localBulkWriter.appendRow(row);
            }

            System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());

            localBulkWriter.commit(false);
            List<List<String>> batchFiles = localBulkWriter.getBatchFiles();
            System.out.printf("Local writer done! output local files: %s%n", batchFiles);
        } catch (Exception e) {
            System.out.println("Local writer catch exception: " + e);
            throw e;
        }
    }

    private static void parallelAppend(CreateCollectionReq.CollectionSchema collectionSchema) throws Exception {
        System.out.print("\n===================== parallel append ====================");
        LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchema)
                .withLocalPath("/tmp/bulk_writer")
                .withFileType(BulkFileType.PARQUET)
                .withChunkSize(128 * 1024 * 1024)  // 128MB
                .build();

        try (LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam)) {
            List<Thread> threads = new ArrayList<>();
            int threadCount = 10;
            int rowsPerThread = 1000;
            for (int i = 0; i < threadCount; ++i) {
                int current = i;
                Thread thread = new Thread(() -> appendRow(localBulkWriter, current * rowsPerThread, (current + 1) * rowsPerThread));
                threads.add(thread);
                thread.start();
                System.out.printf("Thread %s started%n", thread.getName());
            }

            for (Thread thread : threads) {
                thread.join();
                System.out.printf("Thread %s finished%n", thread.getName());
            }

            System.out.println(localBulkWriter.getTotalRowCount() + " rows appends");
            localBulkWriter.commit(false);
            System.out.printf("Append finished, %s rows%n", threadCount * rowsPerThread);

            long rowCount = 0;
            List<List<String>> batchFiles = localBulkWriter.getBatchFiles();
            for (List<String> batch : batchFiles) {
                for (String filePath : batch) {
                    rowCount += readParquet(filePath);
                }
            }

            Asserts.check(rowCount == threadCount * rowsPerThread, String.format("rowCount %s not equals expected %s", rowCount, threadCount * rowsPerThread));
            System.out.println("Data is correct");
        } catch (Exception e) {
            System.out.println("parallelAppend catch exception: " + e);
            throw e;
        }
    }

    private static long readParquet(String localFilePath) throws Exception {
        final long[] rowCount = {0};
        new ParquetReaderUtils() {
            @Override
            public void readRecord(GenericData.Record record) {
                rowCount[0]++;
                String pathValue = record.get("path").toString();
                String labelValue = record.get("label").toString();
                Asserts.check(pathValue.replace("path_", "").equals(labelValue.replace("label_", "")), String.format("the suffix of %s not equals the suffix of %s", pathValue, labelValue));
            }
        }.readParquet(localFilePath);
        System.out.printf("The file %s contains %s rows. Verify the content...%n", localFilePath, rowCount[0]);
        return rowCount[0];
    }

    private static void appendRow(LocalBulkWriter writer, int begin, int end) {
        try {
            for (int i = begin; i < end; ++i) {
                JsonObject row = new JsonObject();
                row.addProperty("path", "path_" + i);
                row.add("vector", GSON_INSTANCE.toJsonTree(GeneratorUtils.genFloatVector(DIM)));
                row.addProperty("label", "label_" + i);

                writer.appendRow(row);
                if (i % 100 == 0) {
                    System.out.printf("%s inserted %s items%n", Thread.currentThread().getName(), i - begin);
                }
            }
        } catch (Exception e) {
            System.out.println("failed to append row!");
        }
    }

    private static void readCsvSampleData(String filePath, BulkWriter writer) throws IOException, InterruptedException {
        ClassLoader classLoader = BulkWriterLocalExample.class.getClassLoader();
        URL resourceUrl = classLoader.getResource(filePath);
        filePath = new File(resourceUrl.getFile()).getAbsolutePath();

        CsvMapper csvMapper = new CsvMapper();

        File csvFile = new File(filePath);
        CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
        Iterator<CsvDataObject> iterator = csvMapper.readerFor(CsvDataObject.class).with(csvSchema).readValues(csvFile);
        while (iterator.hasNext()) {
            CsvDataObject dataObject = iterator.next();
            JsonObject row = new JsonObject();

            row.add("vector", GSON_INSTANCE.toJsonTree(dataObject.toFloatArray()));
            row.addProperty("label", dataObject.getLabel());
            row.addProperty("path", dataObject.getPath());

            writer.appendRow(row);
        }
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

    private static CreateCollectionReq.CollectionSchema buildSimpleSchema() {
        CreateCollectionReq.CollectionSchema schemaV2 = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .autoID(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("path")
                .dataType(DataType.VarChar)
                .maxLength(512)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("label")
                .dataType(DataType.VarChar)
                .maxLength(512)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(DIM)
                .build());

        return schemaV2;
    }

    private static void checkMilvusClientIfExist() {
        if (milvusClient == null) {
            String msg = "milvusClient is null. Please initialize it by calling createConnection() first before use.";
            throw new RuntimeException(msg);
        }
    }
}

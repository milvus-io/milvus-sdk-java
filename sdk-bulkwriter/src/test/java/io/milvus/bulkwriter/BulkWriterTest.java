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

package io.milvus.bulkwriter;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.common.utils.ParquetReaderUtils;
import io.milvus.bulkwriter.common.utils.V2AdapterUtils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.avro.generic.GenericData;
import org.apache.commons.text.RandomStringGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.milvus.MilvusContainer;

import java.io.IOException;
import java.util.*;

@Testcontainers(disabledWithoutDocker = true)
public class BulkWriterTest {
    private static MilvusClientV2 client;
    private static RandomStringGenerator generator;
    private static final int DIMENSION = 128;
    private static final TestUtils utils = new TestUtils(DIMENSION);

    @Container
    private static final MilvusContainer milvus = new MilvusContainer("milvusdb/milvus:v2.5.4");

    @BeforeAll
    public static void setUp() {
        ConnectConfig config = ConnectConfig.builder()
                .uri(milvus.getEndpoint())
                .build();
        client = new MilvusClientV2(config);
        generator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();
    }

    @AfterAll
    public static void tearDown() throws InterruptedException {
        if (client != null) {
            client.close(5L);
        }
    }

    CreateCollectionReq.CollectionSchema buildSchema(boolean enableDynamicField) {
        CreateCollectionReq.CollectionSchema schemaV2 = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(enableDynamicField)
                .build();
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("bool_field")
                .dataType(DataType.Bool)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int8_field")
                .dataType(DataType.Int8)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int16_field")
                .dataType(DataType.Int16)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int32_field")
                .dataType(DataType.Int32)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int64_field")
                .dataType(DataType.Int64)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float_field")
                .dataType(DataType.Float)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("double_field")
                .dataType(DataType.Double)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("varchar_field")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("json_field")
                .dataType(DataType.JSON)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("arr_int32_field")
                .dataType(DataType.Array)
                .maxCapacity(20)
                .elementType(DataType.Int32)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("arr_float_field")
                .dataType(DataType.Array)
                .maxCapacity(10)
                .elementType(DataType.Float)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("arr_varchar_field")
                .dataType(DataType.Array)
                .maxLength(50)
                .maxCapacity(5)
                .elementType(DataType.VarChar)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float_vector_field")
                .dataType(DataType.FloatVector)
                .dimension(DIMENSION)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("binary_vector_field")
                .dataType(DataType.BinaryVector)
                .dimension(DIMENSION)
                .build());
        return schemaV2;
    }

    @Test
    void testV2AdapterUtils() {
        CreateCollectionReq.CollectionSchema schemaV2 = buildSchema(true);
        CollectionSchemaParam schemaV1 = V2AdapterUtils.convertV2Schema(schemaV2);
        Assertions.assertEquals(schemaV2.isEnableDynamicField(), schemaV1.isEnableDynamicField());

        List<CreateCollectionReq.FieldSchema> fieldSchemaListV2 = schemaV2.getFieldSchemaList();
        Map<String, CreateCollectionReq.FieldSchema> fieldSchemaMapV2 = new HashMap<>();
        for (CreateCollectionReq.FieldSchema field : fieldSchemaListV2) {
            fieldSchemaMapV2.put(field.getName(), field);
        }

        List<FieldType> fieldSchemaListV1 = schemaV1.getFieldTypes();
        for (FieldType fieldSchemaV1 : fieldSchemaListV1) {
            Assertions.assertTrue(fieldSchemaMapV2.containsKey(fieldSchemaV1.getName()));
            CreateCollectionReq.FieldSchema fieldSchemaV2 = fieldSchemaMapV2.get(fieldSchemaV1.getName());
            Assertions.assertEquals(fieldSchemaV2.getDescription(), fieldSchemaV1.getDescription());
            Assertions.assertEquals(fieldSchemaV2.getDataType().name(), fieldSchemaV1.getDataType().name());
            Assertions.assertEquals(fieldSchemaV2.getIsPrimaryKey(), fieldSchemaV1.isPrimaryKey());
            Assertions.assertEquals(fieldSchemaV2.getIsPartitionKey(), fieldSchemaV1.isPartitionKey());
            Assertions.assertEquals(fieldSchemaV2.getIsClusteringKey(), fieldSchemaV1.isClusteringKey());
            Assertions.assertEquals(fieldSchemaV2.getAutoID(), fieldSchemaV1.isAutoID());

            if (fieldSchemaV2.getDimension() != null) {
                Assertions.assertEquals(fieldSchemaV2.getDimension(), fieldSchemaV1.getDimension());
            }
            if(fieldSchemaV2.getDataType() == DataType.VarChar) {
                Assertions.assertEquals(fieldSchemaV2.getMaxLength(), fieldSchemaV1.getMaxLength());
            }

            if (fieldSchemaV2.getDataType() == DataType.Array) {
                Assertions.assertEquals(fieldSchemaV2.getMaxCapacity(), fieldSchemaV1.getMaxCapacity());
                Assertions.assertEquals(fieldSchemaV2.getElementType().name(), fieldSchemaV1.getElementType().name());
                if (fieldSchemaV2.getElementType() == DataType.VarChar) {
                    Assertions.assertEquals(fieldSchemaV2.getMaxLength(), fieldSchemaV1.getMaxLength());
                }
            }
        }
    }

    private static void buildData(BulkWriter writer, int rowCount, boolean isEnableDynamicField) throws IOException, InterruptedException {
        Random random = new Random();
        for (int i = 0; i < rowCount; ++i) {
            JsonObject rowObject = new JsonObject();

            // scalar field
            rowObject.addProperty("bool_field", i % 5 == 0);
            rowObject.addProperty("int8_field", i % 128);
            rowObject.addProperty("int16_field", i % 1000);
            rowObject.addProperty("int32_field", i % 100000);
            rowObject.addProperty("int64_field", i);
            rowObject.addProperty("float_field", i / 3);
            rowObject.addProperty("double_field", i / 7);
            rowObject.addProperty("varchar_field", "varchar_" + i);
            rowObject.addProperty("json_field", String.format("{\"dummy\": %s, \"ok\": \"name_%s\"}", i, i));

            // vector field
            rowObject.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
            rowObject.add("binary_vector_field", JsonUtils.toJsonTree(utils.generateBinaryVector().array()));

            // array field
            rowObject.add("arr_int32_field", JsonUtils.toJsonTree(GeneratorUtils.generatorInt32Value(random.nextInt(20))));
            rowObject.add("arr_float_field", JsonUtils.toJsonTree(GeneratorUtils.generatorFloatValue(random.nextInt(10))));
            rowObject.add("arr_varchar_field", JsonUtils.toJsonTree(GeneratorUtils.generatorVarcharValue(random.nextInt(5), 5)));

            // dynamic fields
            if (isEnableDynamicField) {
                rowObject.addProperty("dynamic", "dynamic_" + i);
            }

            writer.appendRow(rowObject);
        }
    }

    @Test
    void testWriteParquet() {
        try {
            CreateCollectionReq.CollectionSchema schemaV2 = buildSchema(true);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(BulkFileType.PARQUET)
                    .build();
            LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam);
            buildData(localBulkWriter, 10, schemaV2.isEnableDynamicField());

            System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());
            localBulkWriter.commit(false);
            List<List<String>> filePaths = localBulkWriter.getBatchFiles();
            System.out.println(filePaths);
            Assertions.assertEquals(1, filePaths.size());
            Assertions.assertEquals(1, filePaths.get(0).size());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void testWriteJson() {
        try {
            CreateCollectionReq.CollectionSchema schemaV2 = buildSchema(true);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(BulkFileType.JSON)
                    .build();
            LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam);
            buildData(localBulkWriter, 10, schemaV2.isEnableDynamicField());

            System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());
            localBulkWriter.commit(false);
            List<List<String>> filePaths = localBulkWriter.getBatchFiles();
            System.out.println(filePaths);
            Assertions.assertEquals(1, filePaths.size());
            Assertions.assertEquals(1, filePaths.get(0).size());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void testWriteCSV() {
        try {
            CreateCollectionReq.CollectionSchema schemaV2 = buildSchema(true);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(BulkFileType.CSV)
                    .withConfig("sep", ",")
                    .build();
            LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam);
            buildData(localBulkWriter, 10, schemaV2.isEnableDynamicField());

            System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());
            localBulkWriter.commit(false);
            List<List<String>> filePaths = localBulkWriter.getBatchFiles();
            System.out.println(filePaths);
            Assertions.assertEquals(1, filePaths.size());
            Assertions.assertEquals(1, filePaths.get(0).size());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testLocalBulkWriter() {
        // collection schema
        CreateCollectionReq.CollectionSchema schemaV2 = buildSchema(false);

        // local bulkwriter
        LocalBulkWriterParam writerParam = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(schemaV2)
                .withLocalPath("/tmp/bulk_writer")
                .withFileType(BulkFileType.PARQUET)
                .withChunkSize(100 * 1024)
                .build();

        int rowCount = 100;
        List<List<String>> batchFiles = new ArrayList<>();
        try (LocalBulkWriter bulkWriter = new LocalBulkWriter(writerParam)) {
            for (int i = 0; i < rowCount; i++) {
                JsonObject row = new JsonObject();
                row.addProperty("bool_field", i % 3 == 0);
                row.addProperty("int8_field", i%128);
                row.addProperty("int16_field", i%32768);
                row.addProperty("int32_field", i);
                row.addProperty("int64_field", i);
                row.addProperty("float_field", i/3);
                row.addProperty("double_field", i/7);
                row.addProperty("varchar_field", String.format("varchar_%d", i));
                JsonObject obj = new JsonObject();
                obj.addProperty("dummy", i);
                row.add("json_field", obj);
                row.add("arr_varchar_field", JsonUtils.toJsonTree(Lists.newArrayList("aaa", "bbb", "ccc")));
                row.add("arr_int32_field", JsonUtils.toJsonTree(Lists.newArrayList(5, 6, 3, 2, 1)));
                row.add("arr_float_field", JsonUtils.toJsonTree(Lists.newArrayList(0.5, 1.8)));
                row.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
                row.add("binary_vector_field", JsonUtils.toJsonTree(utils.generateBinaryVector().array()));

                bulkWriter.appendRow(row);
            }

            bulkWriter.commit(false);
            List<List<String>> files = bulkWriter.getBatchFiles();
            System.out.printf("LocalBulkWriter done! output local files: %s%n", files);
            Assertions.assertEquals(1, files.size());
            Assertions.assertEquals(files.get(0).size(), 1);
            batchFiles.addAll(files);
        } catch (Exception e) {
            System.out.println("LocalBulkWriter catch exception: " + e);
            Assertions.fail();
        }

        try {
            final int[] counter = {0};
            for (List<String> files : batchFiles) {
                new ParquetReaderUtils() {
                    @Override
                    public void readRecord(GenericData.Record record) {
                        counter[0]++;
                    }
                }.readParquet(files.get(0));
            }
            Assertions.assertEquals(rowCount, counter[0]);
        } catch (Exception e) {
            System.out.println("Verify parquet file catch exception: " + e);
            Assertions.fail();
        }
    }
}

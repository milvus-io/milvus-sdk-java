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

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.common.utils.ParquetReaderUtils;
import io.milvus.bulkwriter.common.utils.V2AdapterUtils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.exception.MilvusException;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

public class BulkWriterTest {
    private static final int DIMENSION = 32;
    private static final TestUtils utils = new TestUtils(DIMENSION);

    private static CollectionSchemaParam buildV1Schema(boolean enableDynamicField) {
        List<FieldType> fieldsSchema = new ArrayList<>();
        fieldsSchema.add(FieldType.newBuilder()
                .withPrimaryKey(true)
                .withAutoID(false)
                .withDataType(io.milvus.grpc.DataType.Int64)
                .withName("id")
                .withDescription("id")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(io.milvus.grpc.DataType.FloatVector)
                .withName("float_vector")
                .withDescription("float_vector")
                .withDimension(DIMENSION)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(io.milvus.grpc.DataType.SparseFloatVector)
                .withName("sparse_vector")
                .withDescription("sparse_vector")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(io.milvus.grpc.DataType.Bool)
                .withName("bool")
                .withDescription("bool")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(io.milvus.grpc.DataType.Double)
                .withName("double")
                .withDescription("double")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(io.milvus.grpc.DataType.VarChar)
                .withName("varchar")
                .withDescription("varchar")
                .withMaxLength(100)
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(io.milvus.grpc.DataType.Int8)
                .withName("int8")
                .withDescription("int8")
                .build());

        fieldsSchema.add(FieldType.newBuilder()
                .withDataType(io.milvus.grpc.DataType.Array)
                .withElementType(io.milvus.grpc.DataType.VarChar)
                .withName("array")
                .withDescription("array")
                .withMaxLength(200)
                .withMaxCapacity(20)
                .build());

        return CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(enableDynamicField)
                .withFieldTypes(fieldsSchema)
                .build();
    }

    private static CreateCollectionReq.CollectionSchema buildV2Schema(boolean enableDynamicField, boolean autoID) {
        CreateCollectionReq.CollectionSchema schemaV2 = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(enableDynamicField)
                .build();
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(autoID)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("bool_field")
                .dataType(DataType.Bool)
                .isNullable(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int8_field")
                .dataType(DataType.Int8)
                .defaultValue((short)8)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int16_field")
                .dataType(DataType.Int16)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int32_field")
                .dataType(DataType.Int32)
                .isNullable(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int64_field")
                .dataType(DataType.Int64)
                .isNullable(true)
                .defaultValue(null)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("float_field")
                .dataType(DataType.Float)
                .isNullable(true)
                .defaultValue(0.618)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("double_field")
                .dataType(DataType.Double)
                .defaultValue(3.141592657)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("varchar_field")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .isNullable(true)
                .defaultValue("default")
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("json_field")
                .dataType(DataType.JSON)
                .isNullable(true)
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
                .isNullable(true)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("arr_varchar_field")
                .dataType(DataType.Array)
                .maxLength(50)
                .maxCapacity(5)
                .elementType(DataType.VarChar)
                .isNullable(true)
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
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("sparse_vector_field")
                .dataType(DataType.SparseFloatVector)
                .build());
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("int8_vector_field")
                .dataType(DataType.Int8Vector)
                .dimension(DIMENSION)
                .build());
        return schemaV2;
    }

    private static List<JsonObject> buildData(int rowCount, boolean isEnableDynamicField, boolean autoID) {
        Random random = new Random();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; ++i) {
            JsonObject rowObject = new JsonObject();
            if (!autoID) {
                rowObject.addProperty("id", i);
            }

            // some rows contains null values
            if (i%5 == 0) {
                // scalar field
//                rowObject.addProperty("bool_field", true); // nullable, no need to provide
//                rowObject.add("int8_field", null); // has default value, no need to provide
                rowObject.addProperty("int16_field", i % 1000); // not nullable, no default value, must provide
//                rowObject.add("int32_field", null); // nullable, no need to provide
//                rowObject.add("int64_field", null); // nullable, default value is null, no need to provide
//                rowObject.add("float_field", null); // nullable, has default value, no need to provide
//                rowObject.add("double_field", null); // has default value, no need to provide
//                rowObject.add("varchar_field", null); // nullable, has default value, no need to provide
//                rowObject.add("json_field", null); // nullable, no need to provide

                // array field
                rowObject.add("arr_int32_field", JsonUtils.toJsonTree(GeneratorUtils.generatorInt32Value(random.nextInt(4)))); // not nullable, must provide
//                rowObject.add("arr_float_field", null); // nullable, no need to provide
//                rowObject.add("arr_varchar_field", null); // nullable, no need to provide
            } else if (i%3 == 0) {
                // scalar field
                rowObject.add("bool_field", null); // nullable, set null is ok
                rowObject.add("int8_field", null); // has default value, set null to get default
                rowObject.addProperty("int16_field", i % 1000); // not nullable, no default value, must provide
                rowObject.add("int32_field", null); // nullable, set null is ok
                rowObject.add("int64_field", null); // nullable, set null is ok
                rowObject.add("float_field", null); // nullable, has default value, set null is ok
                rowObject.add("double_field", null); // has default value, set null is ok
                rowObject.add("varchar_field", null); // nullable, has default value, set null is ok
                rowObject.add("json_field", null); // nullable, set null is ok

                // array field
                rowObject.add("arr_int32_field", JsonUtils.toJsonTree(GeneratorUtils.generatorInt32Value(random.nextInt(3)))); // not nullable, must provide
                rowObject.add("arr_float_field", null); // nullable, set null is ok
                rowObject.add("arr_varchar_field", null); // nullable, set null is ok
            } else {
                // scalar field
                rowObject.addProperty("bool_field", i % 2 == 0);
                rowObject.addProperty("int8_field", i % 128);
                rowObject.addProperty("int16_field", i % 1000);
                rowObject.addProperty("int32_field", i % 100000);
                rowObject.addProperty("int64_field", i);
                rowObject.addProperty("float_field", i / 3);
                rowObject.addProperty("double_field", i / 7);
                rowObject.addProperty("varchar_field", "varchar_" + i);
                rowObject.addProperty("json_field", String.format("{\"dummy\": %s, \"ok\": \"name_%s\"}", i, i));

                // array field
                rowObject.add("arr_int32_field", JsonUtils.toJsonTree(GeneratorUtils.generatorInt32Value(random.nextInt(5))));
                rowObject.add("arr_float_field", JsonUtils.toJsonTree(GeneratorUtils.generatorFloatValue(random.nextInt(4))));
                rowObject.add("arr_varchar_field", JsonUtils.toJsonTree(GeneratorUtils.generatorVarcharValue(random.nextInt(3), 5)));

                // dynamic fields
                if (isEnableDynamicField) {
                    rowObject.addProperty("dynamic", "dynamic_" + i);
                }
            }

            // vector field
            rowObject.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
            rowObject.add("binary_vector_field", JsonUtils.toJsonTree(utils.generateBinaryVector().array()));
            rowObject.add("sparse_vector_field", JsonUtils.toJsonTree(utils.generateSparseVector()));
            rowObject.add("int8_vector_field", JsonUtils.toJsonTree(utils.generateInt8Vector().array()));

            rows.add(rowObject);
        }
        return rows;
    }

    private static void writeData(BulkWriter writer, List<JsonObject> rows) throws IOException, InterruptedException {
        for (JsonObject row : rows) {
            writer.appendRow(row);
        }
    }

    private static JsonElement constructJsonPrimitive(Object obj) {
        if (obj == null ) {
            return JsonNull.INSTANCE;
        } else if (obj instanceof Boolean) {
            return new JsonPrimitive((Boolean)obj);
        } else if (obj instanceof Short || obj instanceof Integer || obj instanceof Long ||
                obj instanceof Float || obj instanceof Double) {
            return new JsonPrimitive((Number) obj);
        } else if (obj instanceof String) {
            return new JsonPrimitive((String)obj);
        }

        Assertions.fail("Default value is illegal");
        return null;
    }

    @Test
    void testV2AdapterUtils() {
        CollectionSchemaParam schemaV1 = buildV1Schema(true);
        CreateCollectionReq.CollectionSchema schemaV2 = V2AdapterUtils.convertV1Schema(schemaV1);
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

    @Test
    void testAppend() {
        boolean autoID = true;
        boolean enableDynamicField = true;
        List<BulkFileType> fileTypes = Arrays.asList(BulkFileType.PARQUET, BulkFileType.CSV, BulkFileType.JSON);
        for (BulkFileType fileType : fileTypes) {
            CreateCollectionReq.CollectionSchema schemaV2 = buildV2Schema(enableDynamicField, autoID);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(fileType)
                    .build();
            try(LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam)) {
                JsonObject rowObject = new JsonObject();
                rowObject.addProperty("bool_field", true);
                rowObject.addProperty("int8_field", 1);
//            rowObject.addProperty("int16_field", 2); // a field missed
                rowObject.addProperty("int32_field", 3);
                rowObject.addProperty("int64_field", 4);
                rowObject.addProperty("float_field", 5);
                rowObject.addProperty("double_field", 6);
                rowObject.addProperty("varchar_field", "dummy");
                rowObject.addProperty("json_field", "{}");
                rowObject.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
                rowObject.add("binary_vector_field", JsonUtils.toJsonTree(utils.generateBinaryVector().array()));
                rowObject.add("sparse_vector_field", JsonUtils.toJsonTree(utils.generateSparseVector()));
                rowObject.add("int8_vector_field", JsonUtils.toJsonTree(utils.generateInt8Vector().array()));
                rowObject.add("arr_int32_field", JsonUtils.toJsonTree(GeneratorUtils.generatorInt32Value(2)));
                rowObject.add("arr_float_field", JsonUtils.toJsonTree(GeneratorUtils.generatorFloatValue(3)));
                rowObject.add("arr_varchar_field", JsonUtils.toJsonTree(GeneratorUtils.generatorVarcharValue(4, 5)));

                // a field missed, expect throwing an exception
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // id is auto_id, no need to input, expect throwing an exception
                rowObject.addProperty("id", 1);
                rowObject.addProperty("int16_field", 2);
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // set null value for non-nullable field, expect throwing an exception
                rowObject.remove("id");
                rowObject.add("int16_field", null);
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // set valid value for dynamic field
                rowObject.addProperty("int16_field", 16);
                JsonObject dy = new JsonObject();
                dy.addProperty("dummy", 2);
                rowObject.add("$meta", dy);
                localBulkWriter.appendRow(rowObject);

                // set invalid value for dynamic field, expect throwing an exception
                rowObject.addProperty("$meta", 6);
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // set incorrect dimension vector, expect throwing an exception
                rowObject.remove("$meta");
                rowObject.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION-1)));
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // set incorrect sparse vector, expect throwing an exception
                rowObject.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
                rowObject.add("sparse_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // set incorrect value type for scalar field, expect throwing an exception
                rowObject.add("sparse_vector_field", JsonUtils.toJsonTree(utils.generateSparseVector()));
                rowObject.addProperty("float_field", Boolean.TRUE);
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // set incorrect type for varchar field, expect throwing an exception
                rowObject.addProperty("float_field", 2.5);
                rowObject.addProperty("varchar_field", 2.5);
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));

                // set incorrect value type for int8 vector field, expect throwing an exception
                rowObject.addProperty("varchar_field", "dummy");
                rowObject.addProperty("int8_vector_field", Boolean.TRUE);
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, ()->localBulkWriter.appendRow(rowObject));
            } catch (Exception e) {
                Assertions.fail(e.getMessage());
            }
        }
    }

    @Test
    void testWriteJson() {
        try {
            boolean autoID = true;
            boolean enableDynamicField = true;
            CreateCollectionReq.CollectionSchema schemaV2 = buildV2Schema(enableDynamicField, autoID);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(BulkFileType.JSON)
                    .build();
            LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam);
            List<JsonObject> rows = buildData(10, enableDynamicField, autoID);
            writeData(localBulkWriter, rows);

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
            boolean autoID = true;
            boolean enableDynamicField = true;
            CreateCollectionReq.CollectionSchema schemaV2 = buildV2Schema(enableDynamicField, autoID);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(BulkFileType.CSV)
                    .withConfig("sep", "|")
                    .withConfig("nullkey", "XXX")
                    .build();
            LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam);
            List<JsonObject> rows = buildData(10, enableDynamicField, autoID);
            writeData(localBulkWriter, rows);

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

    private static void verifyJsonString(String s1, String s2) {
        String ss1 = s1.replace("\\\"", "\"").replaceAll("^\"|\"$", "");
        String ss2 = s2.replace("\\\"", "\"").replaceAll("^\"|\"$", "");
        Assertions.assertEquals(ss1, ss2);
    }
    private static void verifyElement(DataType dtype, JsonElement element, Object obj) {
        switch (dtype) {
            case Bool:
                Assertions.assertEquals(element.getAsBoolean(), obj);
                break;
            case Int8:
            case Int16:
            case Int32:
                Assertions.assertEquals(element.getAsInt(), obj);
                break;
            case Int64:
                Assertions.assertEquals(element.getAsLong(), obj);
                break;
            case Float:
                Assertions.assertEquals(element.getAsFloat(), obj);
                break;
            case Double:
                Assertions.assertEquals(element.getAsDouble(), obj);
                break;
            case VarChar:
            case Geometry:
            case Timestamptz:
            case JSON:
                verifyJsonString(element.getAsString(), ((Utf8)obj).toString());
                break;
            case SparseFloatVector:
                verifyJsonString(element.toString(), ((Utf8)obj).toString());
                break;
            default:
                break;
        }
    }

    private static void verifyRow(List<CreateCollectionReq.FieldSchema> fieldsList, List<JsonObject> originalData, GenericData.Record readRow) {
        long id = (long)readRow.get("id");
        JsonObject expectedRow = originalData.get((int)id);
        for (CreateCollectionReq.FieldSchema field : fieldsList) {
            String fieldName = field.getName();
            Object readValue = readRow.get(fieldName);
            JsonElement expectedEle = expectedRow.get(fieldName);
            if (readValue == null) {
                if (field.getIsNullable()) {
                    Assertions.assertTrue(expectedEle.isJsonNull());
                    continue;
                }
            } else if (expectedEle.isJsonNull()) {
                if (field.getDefaultValue() != null) {
                    expectedEle = constructJsonPrimitive(field.getDefaultValue());
                }
            }

            DataType dtype = field.getDataType();
            switch (dtype) {
                case Array:
                case FloatVector:
                case BinaryVector:
                case Float16Vector:
                case BFloat16Vector:
                    if (!(readValue instanceof List)) {
                        Assertions.fail("Array field type unmatched");
                    }
                    List<JsonElement> jsonArr = expectedEle.getAsJsonArray().asList();
                    List<GenericData.Record> objArr = (List<GenericData.Record>)readValue;
                    if (jsonArr.size() != objArr.size()) {
                        Assertions.fail("Array field length unmatched");
                    }
                    DataType elementType = field.getElementType();
                    switch (dtype) {
                        case FloatVector:
                            elementType = DataType.Float;
                            break;
                        case BFloat16Vector:
                        case Float16Vector:
                        case BinaryVector:
                            elementType = DataType.Int32;
                            break;
                        default:
                            break;
                    }
                    for (int i = 0; i < jsonArr.size(); i++) {
                        GenericData.Record value = objArr.get(i);
                        verifyElement(elementType, jsonArr.get(i), value.get("element"));
                    }
                    break;
                default:
                    verifyElement(dtype, expectedEle, readValue);
                    break;
            }
        }
        System.out.printf("The row of id=%d is correct%n", id);
    }

    @Test
    public void testWriteParquet() {
        // collection schema
        boolean autoID = false;
        boolean enableDynamicField = false;
        CreateCollectionReq.CollectionSchema schemaV2 = buildV2Schema(enableDynamicField, autoID);

        // local bulkwriter
        LocalBulkWriterParam writerParam = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(schemaV2)
                .withLocalPath("/tmp/bulk_writer")
                .withFileType(BulkFileType.PARQUET)
                .withChunkSize(100 * 1024)
                .build();

        int rowCount = 10;
        List<JsonObject> originalData = new ArrayList<>();
        List<List<String>> batchFiles = new ArrayList<>();
        try (LocalBulkWriter bulkWriter = new LocalBulkWriter(writerParam)) {
            originalData = buildData(10, enableDynamicField, autoID);
            writeData(bulkWriter, originalData);

            bulkWriter.commit(false);
            List<List<String>> files = bulkWriter.getBatchFiles();
            System.out.printf("LocalBulkWriter done! output local files: %s%n", files);
            Assertions.assertEquals(1, files.size());
            Assertions.assertEquals(files.get(0).size(), 1);
            batchFiles.addAll(files);
        } catch (Exception e) {
            System.out.println("LocalBulkWriter catch exception: " + e);
            e.printStackTrace();
            Assertions.fail();
        }

        // verify data from the parquet file
        try {
            final int[] counter = {0};
            for (List<String> files : batchFiles) {
                List<JsonObject> finalOriginalData = originalData;
                List<CreateCollectionReq.FieldSchema> fieldsList = schemaV2.getFieldSchemaList();
                new ParquetReaderUtils() {
                    @Override
                    public void readRecord(GenericData.Record record) {
                        counter[0]++;
                        verifyRow(fieldsList, finalOriginalData, record);
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

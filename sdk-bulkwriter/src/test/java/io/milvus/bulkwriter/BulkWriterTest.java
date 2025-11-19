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

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.common.utils.GeneratorUtils;
import io.milvus.bulkwriter.common.utils.ParquetReaderUtils;
import io.milvus.bulkwriter.common.utils.V2AdapterUtils;
import io.milvus.common.utils.JsonUtils;
import io.milvus.exception.MilvusException;
import io.milvus.param.Constant;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
                .defaultValue((short) 8)
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
        schemaV2.addField(AddFieldReq.builder()
                .fieldName("struct_field")
                .dataType(DataType.Array)
                .elementType(DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_bool")
                        .dataType(DataType.Bool)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_int8")
                        .dataType(DataType.Int8)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_int16")
                        .dataType(DataType.Int16)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_int32")
                        .dataType(DataType.Int32)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_int64")
                        .dataType(DataType.Int64)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_float")
                        .dataType(DataType.Float)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_double")
                        .dataType(DataType.Double)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_string")
                        .dataType(DataType.VarChar)
                        .maxLength(100)
                        .build())
                .addStructField(AddFieldReq.builder()
                        .fieldName("st_float_vector")
                        .dataType(DataType.FloatVector)
                        .dimension(DIMENSION)
                        .build())
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
            if (i % 5 == 0) {
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
                rowObject.add("arr_varchar_field", JsonUtils.toJsonTree(new ArrayList<>())); //empty array
            } else if (i % 3 == 0) {
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

            // struct field
            List<JsonObject> structList = new ArrayList<>();
            for (int k = 0; k < i % 4 + 1; k++) {
                JsonObject st = new JsonObject();
                st.addProperty("st_bool", (i + k) % 3 == 0);
                st.addProperty("st_int8", (i + k) % 128);
                st.addProperty("st_int16", (i + k) % 16384);
                st.addProperty("st_int32", (i + k) % 65536);
                st.addProperty("st_int64", i + k);
                st.addProperty("st_float", (float) (i + k) / 4);
                st.addProperty("st_double", (i + k) / 3);
                st.addProperty("st_string", String.format("dummy_%d", i + k));
                st.add("st_float_vector", JsonUtils.toJsonTree(utils.generateFloatVector()));
                structList.add(st);
            }
            rowObject.add("struct_field", JsonUtils.toJsonTree(structList));

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
        if (obj == null) {
            return JsonNull.INSTANCE;
        } else if (obj instanceof Boolean) {
            return new JsonPrimitive((Boolean) obj);
        } else if (obj instanceof Short || obj instanceof Integer || obj instanceof Long ||
                obj instanceof Float || obj instanceof Double) {
            return new JsonPrimitive((Number) obj);
        } else if (obj instanceof String) {
            return new JsonPrimitive((String) obj);
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
            if (fieldSchemaV2.getDataType() == DataType.VarChar) {
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
            try (LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam)) {
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
                rowObject.add("struct_field", JsonUtils.toJsonTree(new ArrayList<>()));

                // a field missed, expect throwing an exception
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // id is auto_id, no need to input, expect throwing an exception
                rowObject.addProperty("id", 1);
                rowObject.addProperty("int16_field", 2);
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // set null value for non-nullable field, expect throwing an exception
                rowObject.remove("id");
                rowObject.add("int16_field", null);
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // set valid value for dynamic field
                rowObject.addProperty("int16_field", 16);
                JsonObject dy = new JsonObject();
                dy.addProperty("dummy", 2);
                rowObject.add("$meta", dy);
                localBulkWriter.appendRow(rowObject);

                // set invalid value for dynamic field, expect throwing an exception
                rowObject.addProperty("$meta", 6);
//            localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // set incorrect dimension vector, expect throwing an exception
                rowObject.remove("$meta");
                rowObject.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector(DIMENSION - 1)));
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // set incorrect sparse vector, expect throwing an exception
                rowObject.add("float_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
                rowObject.add("sparse_vector_field", JsonUtils.toJsonTree(utils.generateFloatVector()));
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // set incorrect value type for scalar field, expect throwing an exception
                rowObject.add("sparse_vector_field", JsonUtils.toJsonTree(utils.generateSparseVector()));
                rowObject.addProperty("float_field", Boolean.TRUE);
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // set incorrect type for varchar field, expect throwing an exception
                rowObject.addProperty("float_field", 2.5);
                rowObject.addProperty("varchar_field", 2.5);
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));

                // set incorrect value type for int8 vector field, expect throwing an exception
                rowObject.addProperty("varchar_field", "dummy");
                rowObject.addProperty("int8_vector_field", Boolean.TRUE);
//                localBulkWriter.appendRow(rowObject);
                Assertions.assertThrows(MilvusException.class, () -> localBulkWriter.appendRow(rowObject));
            } catch (Exception e) {
                Assertions.fail(e.getMessage());
            }
        }
    }

    private static void compareJsonArray(JsonElement j1, JsonElement j2, DataType dt) {
        if (j1.isJsonNull()) {
            Assertions.assertTrue(j2.isJsonNull());
            return;
        }
        Assertions.assertTrue(j1.isJsonArray());
        Assertions.assertTrue(j2.isJsonArray());
        List<JsonElement> a1 = j1.getAsJsonArray().asList();
        List<JsonElement> a2 = j2.getAsJsonArray().asList();
        Assertions.assertEquals(a1.size(), a2.size());
        for (int i = 0; i < a1.size(); i++) {
            switch (dt) {
                case Bool:
                    Assertions.assertEquals(a1.get(i).getAsBoolean(), a2.get(i).getAsBoolean());
                    break;
                case Int8:
                case Int16:
                case Int32:
                    Assertions.assertEquals(a1.get(i).getAsInt(), a2.get(i).getAsInt());
                    break;
                case Float:
                    Assertions.assertEquals(a1.get(i).getAsFloat(), a2.get(i).getAsFloat());
                    break;
                case Double:
                    Assertions.assertEquals(a1.get(i).getAsDouble(), a2.get(i).getAsDouble());
                    break;
                case VarChar:
                    Assertions.assertEquals(a1.get(i).getAsString(), a2.get(i).getAsString());
                default:
                    Assertions.assertEquals(a1.get(i), a2.get(i));
                    break;
            }
        }
    }

    @Test
    void testWriteJson() {
        try {
            final boolean autoID = true;
            final boolean enableDynamicField = true;
            final int row_count = 10;
            CreateCollectionReq.CollectionSchema schemaV2 = buildV2Schema(enableDynamicField, autoID);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(BulkFileType.JSON)
                    .build();
            LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam);
            List<JsonObject> rows = buildData(row_count, enableDynamicField, autoID);
            writeData(localBulkWriter, rows);

            System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());
            localBulkWriter.commit(false);
            List<List<String>> filePaths = localBulkWriter.getBatchFiles();
            System.out.println(filePaths);
            Assertions.assertEquals(1, filePaths.size());
            Assertions.assertEquals(1, filePaths.get(0).size());

            Gson gson = new Gson();
            Reader reader = Files.newBufferedReader(Paths.get(filePaths.get(0).get(0)));
            List<JsonObject> lines = gson.fromJson(reader, new TypeToken<List<JsonObject>>() {
            }.getType());
            Assertions.assertEquals(row_count, lines.size());
            for (int i = 0; i < lines.size(); i++) {
                JsonObject expectedDict = rows.get(i);
                JsonObject readDict = lines.get(i);
                Assertions.assertTrue(readDict.has(Constant.DYNAMIC_FIELD_NAME));
                Assertions.assertTrue(expectedDict.keySet().size() == readDict.keySet().size() ||
                        expectedDict.keySet().size() + 1 == readDict.keySet().size());
                for (String key : readDict.keySet()) {
                    if (key.equals(Constant.DYNAMIC_FIELD_NAME)) {
                        continue;
                    }

                    JsonElement expectVal = expectedDict.get(key);
                    JsonElement readVal = readDict.get(key);
                    if (key.equals("sparse_vector_field")) {
                        JsonObject expectSparse = (JsonObject) expectVal;
                        JsonObject readSparse = (JsonObject) readVal;
                        Assertions.assertEquals(expectSparse.keySet().size(), readSparse.keySet().size());
                        for (String id : expectSparse.keySet()) {
                            Assertions.assertTrue(readSparse.has(id));
                            Assertions.assertEquals(expectSparse.get(id).getAsFloat(), readSparse.get(id).getAsFloat());
                        }
                    } else if (key.equals("float_vector_field") || key.equals("arr_float_field")) {
                        compareJsonArray(expectVal, readVal, DataType.Float);
                    } else if (key.equals("json_field")) {
                        if (expectVal.isJsonNull()) {
                            Assertions.assertTrue(readVal.isJsonNull());
                        } else {
                            String str = expectVal.toString();
                            Assertions.assertEquals(str, readVal.getAsString());
                        }
                    } else if (key.equals("arr_varchar_field")) {
                        compareJsonArray(expectVal, readVal, DataType.VarChar);
                    } else if (key.equals("struct_field")) {
                        JsonArray expectStructs = (JsonArray) expectVal;
                        JsonArray readStructs = (JsonArray) readVal;
                        Assertions.assertEquals(expectStructs.size(), readStructs.size());
                        for (int k = 0; k < expectStructs.size(); k++) {
                            JsonObject expectStruct = (JsonObject) expectStructs.get(k);
                            JsonObject readStruct = (JsonObject) readStructs.get(k);
                            Assertions.assertEquals(expectStruct.keySet().size(), readStruct.keySet().size());
                            for (String id : expectStruct.keySet()) {
                                Assertions.assertTrue(readStruct.has(id));
                                if (id.equals("st_float_vector")) {
                                    compareJsonArray(expectStruct.get(id), readStruct.get(id), DataType.Float);
                                } else {
                                    Assertions.assertEquals(expectStruct.get(id), readStruct.get(id));
                                }
                            }
                        }
                    } else {
                        Assertions.assertEquals(expectVal, readVal);
                    }
                }
            }
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    void testWriteCSV() {
        try {
            final boolean autoID = true;
            final boolean enableDynamicField = true;
            final int row_count = 10;
            final String nullKey = "XXX";
            CreateCollectionReq.CollectionSchema schemaV2 = buildV2Schema(enableDynamicField, autoID);
            LocalBulkWriterParam bulkWriterParam = LocalBulkWriterParam.newBuilder()
                    .withCollectionSchema(schemaV2)
                    .withLocalPath("/tmp/bulk_writer")
                    .withFileType(BulkFileType.CSV)
                    .withConfig("sep", "|")
                    .withConfig("nullkey", nullKey)
                    .build();
            LocalBulkWriter localBulkWriter = new LocalBulkWriter(bulkWriterParam);
            List<JsonObject> rows = buildData(row_count, enableDynamicField, autoID);
            writeData(localBulkWriter, rows);

            System.out.printf("%s rows appends%n", localBulkWriter.getTotalRowCount());
            localBulkWriter.commit(false);
            List<List<String>> filePaths = localBulkWriter.getBatchFiles();
            System.out.println(filePaths);
            Assertions.assertEquals(1, filePaths.size());
            Assertions.assertEquals(1, filePaths.get(0).size());

            try (BufferedReader br = new BufferedReader(new FileReader(filePaths.get(0).get(0)))) {
                String line;
                List<String> header = new ArrayList<>();
                int num_line = 0;
                while ((line = br.readLine()) != null) {
                    if (header.isEmpty()) {
                        header = Arrays.asList(line.split("\\|"));
                        Assertions.assertTrue(header.contains(Constant.DYNAMIC_FIELD_NAME));
                        continue;
                    }
                    JsonObject expectedRow = rows.get(num_line++);
                    String[] values = line.split("\\|");
                    Assertions.assertTrue(values.length == expectedRow.size() || values.length == expectedRow.size() + 1);
                    for (int i = 0; i < header.size(); i++) {
                        String field = header.get(i);
                        if (field.equals(Constant.DYNAMIC_FIELD_NAME)) {
                            continue;
                        }

                        Assertions.assertTrue(expectedRow.has(field));
                        JsonElement expectEle = expectedRow.get(field);
                        String readStr = values[i];
                        if (expectEle.isJsonNull()) {
                            Assertions.assertEquals(String.format("\"%s\"", nullKey), readStr);
                        } else if (expectEle.isJsonArray()) {
                            if (field.equals("struct_field")) {
                                Gson gson = new Gson();
                                readStr = readStr.substring(1, readStr.length() - 1);
                                readStr = readStr.replace("\"\"", "\"");
                                List<JsonObject> readStructs = gson.fromJson(readStr, new TypeToken<List<JsonObject>>() {
                                }.getType());
                                List<JsonElement> expectStructs = expectEle.getAsJsonArray().asList();
                                Assertions.assertEquals(expectStructs.size(), readStructs.size());
                                for (int k = 0; k < expectStructs.size(); k++) {
                                    JsonObject expectStruct = expectStructs.get(k).getAsJsonObject();
                                    JsonObject readStruct = readStructs.get(k);
                                    for (String key : expectStruct.keySet()) {
                                        Assertions.assertTrue(readStruct.has(key));
                                        if (expectStruct.get(key).isJsonArray()) {
                                            Assertions.assertTrue(readStruct.get(key).isJsonArray());
                                            List<JsonElement> expectVals = expectStruct.get(key).getAsJsonArray().asList();
                                            List<JsonElement> readVals = readStruct.get(key).getAsJsonArray().asList();
                                            Assertions.assertEquals(expectVals.size(), readVals.size());
                                            for (int j = 0; j < expectVals.size(); j++) {
                                                Assertions.assertEquals(expectVals.get(j).getAsFloat(), readVals.get(j).getAsFloat());
                                            }
                                        } else {
                                            Assertions.assertEquals(expectStruct.get(key), readStruct.get(key));
                                        }
                                    }
                                }
                            } else {
                                String expectStr = String.format("\"%s\"", expectEle.getAsJsonArray().toString());
                                readStr = readStr.replace(" ", "");
                                readStr = readStr.replace("\"\"", "\"");
                                Assertions.assertEquals(expectStr, readStr);
                            }
                        } else if (expectEle.isJsonObject()) {
                            String expectStr = String.format("\"%s\"", expectEle.getAsJsonObject().toString());
                            expectStr = expectStr.replace("\"", "\"\"");
                            readStr = String.format("\"%s\"", readStr);
                            Assertions.assertEquals(expectStr, readStr);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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
                verifyJsonString(element.getAsString(), ((Utf8) obj).toString());
                break;
            case SparseFloatVector:
                verifyJsonString(element.toString(), ((Utf8) obj).toString());
                break;
            default:
                break;
        }
    }

    private static void verifyParquetRow(CreateCollectionReq.CollectionSchema schema, List<JsonObject> originalData, GenericData.Record readRow) {
        long id = (long) readRow.get("id");
        JsonObject expectedRow = originalData.get((int) id);
        List<CreateCollectionReq.FieldSchema> fieldsList = schema.getFieldSchemaList();
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
                    List<GenericData.Record> objArr = (List<GenericData.Record>) readValue;
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

        List<CreateCollectionReq.StructFieldSchema> structList = schema.getStructFields();
        for (CreateCollectionReq.StructFieldSchema struct : structList) {
            String fieldName = struct.getName();
            Object readValue = readRow.get(fieldName);
            Assertions.assertInstanceOf(List.class, readValue);
            List<?> readStructs = (List<?>) readValue;
            JsonElement expectedEle = expectedRow.get(fieldName);
            Assertions.assertNotNull(expectedEle);
            JsonArray jsonArr = expectedEle.getAsJsonArray();
            Assertions.assertNotNull(jsonArr);
            List<JsonElement> jsonList = jsonArr.asList();
            Assertions.assertEquals(jsonList.size(), readStructs.size());

            for (int i = 0; i < jsonList.size(); i++) {
                JsonObject expectedDict = (JsonObject) jsonList.get(i);
                GenericData.Record readDict = (GenericData.Record) readStructs.get(i);
                readDict = (GenericData.Record) readDict.get("element");
                for (String key : expectedDict.keySet()) {
                    Assertions.assertTrue(readDict.hasField(key));
                }

                Object expectedVal = expectedDict.get("st_bool").getAsBoolean();
                Object readVal = readDict.get("st_bool");
                Assertions.assertEquals(expectedVal, readVal);

                expectedVal = expectedDict.get("st_int8").getAsInt();
                readVal = readDict.get("st_int8");
                Assertions.assertEquals(expectedVal, readVal);

                expectedVal = expectedDict.get("st_int16").getAsInt();
                readVal = readDict.get("st_int16");
                Assertions.assertEquals(expectedVal, readVal);

                expectedVal = expectedDict.get("st_int32").getAsInt();
                readVal = readDict.get("st_int32");
                Assertions.assertEquals(expectedVal, readVal);

                expectedVal = expectedDict.get("st_int64").getAsLong();
                readVal = readDict.get("st_int64");
                Assertions.assertEquals(expectedVal, readVal);

                expectedVal = expectedDict.get("st_float").getAsFloat();
                readVal = readDict.get("st_float");
                Assertions.assertEquals(expectedVal, readVal);

                expectedVal = expectedDict.get("st_double").getAsDouble();
                readVal = readDict.get("st_double");
                Assertions.assertEquals(expectedVal, readVal);

                expectedVal = expectedDict.get("st_string").getAsString();
                Utf8 utf = (Utf8) readDict.get("st_string");
                String decodedString = new String(utf.getBytes(), StandardCharsets.UTF_8);
                Assertions.assertEquals(expectedVal, decodedString);

                List<JsonElement> expectedArr = expectedDict.get("st_float_vector").getAsJsonArray().asList();
                List<?> readArr = (List<?>) readDict.get("st_float_vector");
                Assertions.assertEquals(expectedArr.size(), readArr.size());
                for (int k = 0; k < readArr.size(); k++) {
                    expectedVal = expectedArr.get(k).getAsFloat();
                    readVal = ((GenericData.Record) readArr.get(k)).get("element");
                    Assertions.assertEquals(expectedVal, readVal);
                }
            }
        }
        System.out.printf("The row of id=%d is correct%n", id);
    }

    @Test
    public void testWriteParquet() {
        // collection schema
        final boolean autoID = false;
        final boolean enableDynamicField = false;
        final int rowCount = 10;
        CreateCollectionReq.CollectionSchema schemaV2 = buildV2Schema(enableDynamicField, autoID);

        // local bulkwriter
        LocalBulkWriterParam writerParam = LocalBulkWriterParam.newBuilder()
                .withCollectionSchema(schemaV2)
                .withLocalPath("/tmp/bulk_writer")
                .withFileType(BulkFileType.PARQUET)
                .withChunkSize(100 * 1024)
                .build();

        List<JsonObject> originalData = new ArrayList<>();
        List<List<String>> batchFiles = new ArrayList<>();
        try (LocalBulkWriter bulkWriter = new LocalBulkWriter(writerParam)) {
            originalData = buildData(rowCount, enableDynamicField, autoID);
            writeData(bulkWriter, originalData);

            bulkWriter.commit(false);
            List<List<String>> files = bulkWriter.getBatchFiles();
            System.out.printf("LocalBulkWriter done! output local files: %s%n", files);
            Assertions.assertEquals(1, files.size());
            Assertions.assertEquals(1, files.get(0).size());
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
                new ParquetReaderUtils() {
                    @Override
                    public void readRecord(GenericData.Record record) {
                        counter[0]++;
                        verifyParquetRow(schemaV2, finalOriginalData, record);
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

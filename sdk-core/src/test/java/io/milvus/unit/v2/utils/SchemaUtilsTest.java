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

package io.milvus.unit.v2.utils;
import io.milvus.v2.utils.SchemaUtils;

import io.milvus.grpc.*;
import io.milvus.exception.ParamException;
import io.milvus.param.Constant;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.*;

@Tag("unit")
public class SchemaUtilsTest {
    private CreateCollectionReq.CollectionSchema buildSchema() {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isPrimaryKey(Boolean.TRUE)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("bool_field")
                .dataType(io.milvus.v2.common.DataType.Bool)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int8_field")
                .dataType(io.milvus.v2.common.DataType.Int8)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int16_field")
                .dataType(io.milvus.v2.common.DataType.Int16)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int32_field")
                .dataType(io.milvus.v2.common.DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("int64_field")
                .dataType(io.milvus.v2.common.DataType.Int64)
                .defaultValue(888L)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_field")
                .dataType(io.milvus.v2.common.DataType.Float)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("double_field")
                .dataType(io.milvus.v2.common.DataType.Double)
                .build());
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("type", "english");
        Map<String, Object> multiAnalyzerParams = new HashMap<>();
        multiAnalyzerParams.put("by_field", "language");
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("varchar_field")
                .dataType(io.milvus.v2.common.DataType.VarChar)
                .maxLength(1000)
                .enableAnalyzer(true)
                .analyzerParams(analyzerParams)
                .multiAnalyzerParams(multiAnalyzerParams)
                .enableMatch(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("json_field")
                .dataType(io.milvus.v2.common.DataType.JSON)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_int_field")
                .dataType(io.milvus.v2.common.DataType.Array)
                .maxCapacity(50)
                .elementType(io.milvus.v2.common.DataType.Int32)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_float_field")
                .dataType(io.milvus.v2.common.DataType.Array)
                .maxCapacity(20)
                .elementType(io.milvus.v2.common.DataType.Float)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("arr_varchar_field")
                .dataType(io.milvus.v2.common.DataType.Array)
                .maxCapacity(10)
                .elementType(io.milvus.v2.common.DataType.VarChar)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float_vector_field")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(128)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("binary_vector_field")
                .dataType(io.milvus.v2.common.DataType.BinaryVector)
                .dimension(64)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("float16_vector_field")
                .dataType(io.milvus.v2.common.DataType.Float16Vector)
                .dimension(256)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("bfloat16_vector_field")
                .dataType(io.milvus.v2.common.DataType.BFloat16Vector)
                .dimension(512)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("sparse_vector_field")
                .dataType(io.milvus.v2.common.DataType.SparseFloatVector)
                .build());

        collectionSchema.addFunction(CreateCollectionReq.Function.builder()
                .functionType(io.milvus.common.clientenum.FunctionType.BM25)
                .name("function_bm25")
                .inputFieldNames(Collections.singletonList("varchar_field"))
                .outputFieldNames(Collections.singletonList("sparse_vector_field"))
                .build());

        return collectionSchema;
    }

    @Test
    void testCheckNullEmptyString() {
        Assertions.assertDoesNotThrow(() -> SchemaUtils.checkNullEmptyString("value", "field"));
        Assertions.assertThrows(MilvusClientException.class, () -> SchemaUtils.checkNullEmptyString("", "field"));
        Assertions.assertThrows(MilvusClientException.class, () -> SchemaUtils.checkNullEmptyString(null, "field"));
    }

    @Test
    void testConvertFromGrpcCollectionSchemaAndStructFieldSchema() {
        FieldSchema idField = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .setIsPrimaryKey(true)
                .build();
        StructArrayFieldSchema structField = StructArrayFieldSchema.newBuilder()
                .setName("clips")
                .setNullable(true)
                .addFields(FieldSchema.newBuilder()
                        .setName("score")
                        .setDataType(DataType.Float)
                        .setNullable(true)
                        .build())
                .build();
        CollectionSchema schema = CollectionSchema.newBuilder()
                .setEnableDynamicField(true)
                .addFields(idField)
                .addStructArrayFields(structField)
                .build();

        CreateCollectionReq.CollectionSchema sdkSchema = SchemaUtils.convertFromGrpcCollectionSchema(schema);
        Assertions.assertTrue(sdkSchema.isEnableDynamicField());
        Assertions.assertEquals(1, sdkSchema.getFieldSchemaList().size());
        Assertions.assertEquals(1, sdkSchema.getStructFields().size());
        Assertions.assertEquals("clips", sdkSchema.getStructFields().get(0).getName());

        CreateCollectionReq.StructFieldSchema sdkStruct =
                SchemaUtils.convertFromGrpcStructFieldSchema(structField);
        Assertions.assertEquals("clips", sdkStruct.getName());
        Assertions.assertEquals(Boolean.TRUE, sdkStruct.getNullable());
        Assertions.assertEquals(1, sdkStruct.getFields().size());
        Assertions.assertEquals("score", sdkStruct.getFields().get(0).getName());
    }

    @Test
    void testConvertFromGrpcStructFieldSchemaStripsSubFieldPrefix() {
        // a sub-field added via add_collection_field keeps the wire name "clips[note]";
        // convertFromGrpcStructFieldSchema must strip it to the plain name "note"
        StructArrayFieldSchema structField = StructArrayFieldSchema.newBuilder()
                .setName("clips")
                .addFields(FieldSchema.newBuilder()
                        .setName("clips[note]")
                        .setDataType(DataType.VarChar)
                        .setElementNullable(true)
                        .build())
                .addFields(FieldSchema.newBuilder()
                        .setName("score")
                        .setDataType(DataType.Int32)
                        .build())
                .build();

        CreateCollectionReq.StructFieldSchema sdkStruct =
                SchemaUtils.convertFromGrpcStructFieldSchema(structField);
        Assertions.assertEquals("note", sdkStruct.getFields().get(0).getName());
        Assertions.assertEquals("score", sdkStruct.getFields().get(1).getName());
        Assertions.assertTrue(sdkStruct.getFields().get(0).getIsNullable());
    }

    @Test
    void testConvertFieldReqToFieldSchemaAndStructFieldSchema() {
        AddFieldReq fieldReq = AddFieldReq.builder()
                .fieldName("vec")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(128)
                .build();
        CreateCollectionReq.FieldSchema fieldSchema = SchemaUtils.convertFieldReqToFieldSchema(fieldReq);
        Assertions.assertEquals("vec", fieldSchema.getName());
        Assertions.assertEquals(io.milvus.v2.common.DataType.FloatVector, fieldSchema.getDataType());

        AddFieldReq structReq = AddFieldReq.builder()
                .fieldName("clips")
                .dataType(io.milvus.v2.common.DataType.Array)
                .elementType(io.milvus.v2.common.DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName("score")
                        .dataType(io.milvus.v2.common.DataType.Float)
                        .build())
                .build();
        CreateCollectionReq.StructFieldSchema structFieldSchema =
                SchemaUtils.convertFieldReqToStructFieldSchema(structReq);
        Assertions.assertEquals("clips", structFieldSchema.getName());
        Assertions.assertEquals(1, structFieldSchema.getFields().size());
        Assertions.assertEquals("score", structFieldSchema.getFields().get(0).getName());
    }

    @Test
    void testConvertFromGrpcFunction() {
        for (FunctionType type : FunctionType.values()) {
            if (type == FunctionType.UNRECOGNIZED) {
                continue;
            }
            FunctionSchema functionSchema = FunctionSchema.newBuilder()
                    .setName("abc")
                    .setDescription("xxx")
                    .setType(type)
                    .addInputFieldNames("text")
                    .addOutputFieldNames("vec")
                    .addParams(KeyValuePair.newBuilder().setKey("provider").setValue("openai").build())
                    .build();

            CreateCollectionReq.Function func = SchemaUtils.convertFromGrpcFunction(functionSchema);
            Assertions.assertEquals(func.getName(), "abc");
            Assertions.assertEquals(func.getDescription(), "xxx");
            Assertions.assertNotNull(func.getFunctionType());
            Assertions.assertEquals(func.getFunctionType().getCode(), type.getNumber());
            Assertions.assertEquals(func.getInputFieldNames().size(), 1);
            Assertions.assertEquals(func.getInputFieldNames().get(0), "text");
            Assertions.assertEquals(func.getOutputFieldNames().size(), 1);
            Assertions.assertEquals(func.getOutputFieldNames().get(0), "vec");
            Map<String, String> params = func.getParams();
            Assertions.assertTrue(params.containsKey("provider"));
            Assertions.assertEquals(params.get("provider"), "openai");
        }
    }

    @Test
    void testConvertFromGrpcMinHashFunction() {
        FunctionSchema functionSchema = FunctionSchema.newBuilder()
                .setName("text_to_minhash")
                .setDescription("minhash function")
                .setType(FunctionType.MinHash)
                .addInputFieldNames("text")
                .addOutputFieldNames("minhash_signature")
                .addParams(KeyValuePair.newBuilder().setKey("num_hashes").setValue("16").build())
                .build();

        CreateCollectionReq.Function func = SchemaUtils.convertFromGrpcFunction(functionSchema);
        Assertions.assertEquals(io.milvus.common.clientenum.FunctionType.MINHASH, func.getFunctionType());
        Assertions.assertEquals("16", func.getParams().get("num_hashes"));
    }

    @Test
    void testConvertToGrpcMinHashFunction() {
        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .name("text_to_minhash")
                .description("minhash function")
                .functionType(io.milvus.common.clientenum.FunctionType.MINHASH)
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("minhash_signature"))
                .param("num_hashes", "16")
                .build();

        FunctionSchema functionSchema = SchemaUtils.convertToGrpcFunction(function);
        Assertions.assertEquals(FunctionType.MinHash, functionSchema.getType());
        Assertions.assertEquals("16", functionSchema.getParams(0).getValue());
    }

    @Test
    void testConvertFromGrpcUnknownFunctionFallsBackToUnknown() {
        FunctionSchema functionSchema = FunctionSchema.newBuilder()
                .setName("unknown_func")
                .setDescription("unknown function")
                .setTypeValue(999)
                .build();

        CreateCollectionReq.Function func = SchemaUtils.convertFromGrpcFunction(functionSchema);
        Assertions.assertEquals(io.milvus.common.clientenum.FunctionType.UNKNOWN, func.getFunctionType());
    }

    @Test
    void testConvertToGrpcFunction() {
        for (io.milvus.common.clientenum.FunctionType type : io.milvus.common.clientenum.FunctionType.values()) {
            CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                    .name("abc")
                    .description("xxx")
                    .functionType(type)
                    .inputFieldNames(Collections.singletonList("text"))
                    .outputFieldNames(Collections.singletonList("vec"))
                    .param("provider", "openai")
                    .build();

            FunctionSchema functionSchema = SchemaUtils.convertToGrpcFunction(function);
            Assertions.assertEquals(functionSchema.getName(), "abc");
            Assertions.assertEquals(functionSchema.getDescription(), "xxx");
            Assertions.assertEquals(functionSchema.getType(), FunctionType.forNumber(type.getCode()));
            Assertions.assertEquals(functionSchema.getInputFieldNamesCount(), 1);
            Assertions.assertEquals(functionSchema.getInputFieldNames(0), "text");
            Assertions.assertEquals(functionSchema.getOutputFieldNamesCount(), 1);
            Assertions.assertEquals(functionSchema.getOutputFieldNames(0), "vec");
            List<KeyValuePair> pairs = functionSchema.getParamsList();
            Assertions.assertEquals(pairs.size(), 1);
            Assertions.assertEquals(pairs.get(0).getKey(), "provider");
            Assertions.assertEquals(pairs.get(0).getValue(), "openai");
        }
    }

    @Test
    void testConvertToGrpcFieldSchema() {
        CreateCollectionReq.CollectionSchema collectionSchema = buildSchema();
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = collectionSchema.getFieldSchemaList();
        for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
            FieldSchema rpcSchema = SchemaUtils.convertToGrpcFieldSchema(fieldSchema);
            Assertions.assertEquals(rpcSchema.getName(), fieldSchema.getName());
            Assertions.assertEquals(rpcSchema.getDescription(), fieldSchema.getDescription());
            Assertions.assertEquals(rpcSchema.getDataType(), DataType.valueOf(fieldSchema.getDataType().name()));
            if (rpcSchema.getDataType() == DataType.Array) {
                Assertions.assertEquals(rpcSchema.getElementType(), DataType.valueOf(fieldSchema.getElementType().name()));
            }
            for (int i = 0; i < rpcSchema.getTypeParamsCount(); i++) {
                KeyValuePair pair = rpcSchema.getTypeParams(i);
                if (pair.getKey() == Constant.VECTOR_DIM) {
                    Assertions.assertEquals(pair.getValue(), fieldSchema.getDimension().toString());
                } else if (pair.getKey() == Constant.VARCHAR_MAX_LENGTH) {
                    Assertions.assertEquals(pair.getValue(), fieldSchema.getMaxLength().toString());
                } else if (pair.getKey() == Constant.ARRAY_MAX_CAPACITY) {
                    Assertions.assertEquals(pair.getValue(), fieldSchema.getMaxCapacity().toString());
                }
            }
            Assertions.assertEquals(rpcSchema.getIsPrimaryKey(), fieldSchema.getIsPrimaryKey());
            Assertions.assertEquals(rpcSchema.getAutoID(), fieldSchema.getAutoID());
            Assertions.assertEquals(rpcSchema.getIsPartitionKey(), fieldSchema.getIsPartitionKey());
            Assertions.assertEquals(rpcSchema.getIsClusteringKey(), fieldSchema.getIsClusteringKey());
            Assertions.assertEquals(rpcSchema.getNullable(), fieldSchema.getIsNullable());

            if (rpcSchema.getName().equals("int64_field")) {
                Assertions.assertEquals(rpcSchema.getDefaultValue().getLongData(), fieldSchema.getDefaultValue());
            } else {
                Assertions.assertEquals(rpcSchema.getDefaultValue(), io.milvus.grpc.ValueField.getDefaultInstance());
            }

            if (rpcSchema.getName().equals("varchar_field")) {
                List<String> keys = new ArrayList<>();
                rpcSchema.getTypeParamsList().forEach((kv) -> keys.add(kv.getKey()));
                Assertions.assertTrue(keys.contains("enable_analyzer"));
                Assertions.assertTrue(keys.contains("enable_match"));
                Assertions.assertTrue(keys.contains("analyzer_params"));
                Assertions.assertTrue(keys.contains("multi_analyzer_params"));
            }
        }
    }

    @Test
    void testConvertToGrpcFieldSchemaTreatsNullNullableAsFalseForAddVectorField() {
        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name("nullable_vector")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(128)
                .isNullable(null)
                .build();

        MilvusClientException exception = Assertions.assertThrows(MilvusClientException.class,
                () -> SchemaUtils.convertToGrpcFieldSchema(fieldSchema, true));
        Assertions.assertEquals(ErrorCode.INVALID_PARAMS, exception.getErrorCode());
        Assertions.assertTrue(exception.getMessage().contains("Vector field must be nullable"));
    }

    @Test
    void testConvertToGrpcStructFieldSchemaPropagatesNullableToSubFields() {
        CreateCollectionReq.StructFieldSchema nullableStruct = CreateCollectionReq.StructFieldSchema.builder()
                .name("metadata")
                .nullable(true)
                .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("score")
                        .dataType(io.milvus.v2.common.DataType.Float)
                        .isNullable(false)
                        .build()))
                .maxCapacity(10)
                .build();
        StructArrayFieldSchema rpc = SchemaUtils.convertToGrpcStructFieldSchema(nullableStruct);
        Assertions.assertTrue(rpc.getNullable());
        // row-level nullability is inherited by every sub-field on the wire
        Assertions.assertTrue(rpc.getFields(0).getNullable());
        // the sub-field's own element-level nullability is mapped to element_nullable
        Assertions.assertFalse(rpc.getFields(0).getElementNullable());

        CreateCollectionReq.StructFieldSchema nonNullableStruct = CreateCollectionReq.StructFieldSchema.builder()
                .name("metadata")
                .nullable(false)
                .fields(Collections.singletonList(CreateCollectionReq.FieldSchema.builder()
                        .name("score")
                        .dataType(io.milvus.v2.common.DataType.Float)
                        .isNullable(true)
                        .build()))
                .maxCapacity(10)
                .build();
        StructArrayFieldSchema rpc2 = SchemaUtils.convertToGrpcStructFieldSchema(nonNullableStruct);
        Assertions.assertFalse(rpc2.getNullable());
        Assertions.assertFalse(rpc2.getFields(0).getNullable());
        // element-level nullability of the sub-field is preserved independently of the struct
        Assertions.assertTrue(rpc2.getFields(0).getElementNullable());
    }

    @Test
    void testStructFieldSchemaAcceptsNullableSubField() {
        CreateCollectionReq.StructFieldSchema structField =
                CreateCollectionReq.StructFieldSchema.builder().name("metadata").build();

        CreateCollectionReq.StructFieldSchema result = structField.addField(AddFieldReq.builder()
                .fieldName("score")
                .dataType(io.milvus.v2.common.DataType.Float)
                .isNullable(Boolean.TRUE)
                .build());
        Assertions.assertEquals(1, result.getFields().size());
        Assertions.assertTrue(result.getFields().get(0).getIsNullable());
    }

    @Test
    void testStructFieldSchemaAcceptsNestedArraySubField() {
        CreateCollectionReq.StructFieldSchema structField =
                CreateCollectionReq.StructFieldSchema.builder().name("metadata").build();

        CreateCollectionReq.StructFieldSchema result = structField.addField(AddFieldReq.builder()
                .fieldName("tags")
                .dataType(io.milvus.v2.common.DataType.Array)
                .elementType(io.milvus.v2.common.DataType.VarChar)
                .maxCapacity(8)
                .maxLength(16)
                .isNullable(Boolean.TRUE)
                .elementNullable(Boolean.TRUE)
                .build());
        Assertions.assertEquals(1, result.getFields().size());
        CreateCollectionReq.FieldSchema tags = result.getFields().get(0);
        Assertions.assertEquals(io.milvus.v2.common.DataType.Array, tags.getDataType());
        Assertions.assertEquals(io.milvus.v2.common.DataType.VarChar, tags.getElementType());
        Assertions.assertTrue(tags.getIsNullable());
        Assertions.assertTrue(tags.getElementNullable());
    }

    @Test
    void testStructFieldSchemaRejectsDirectStructSubField() {
        CreateCollectionReq.StructFieldSchema structField =
                CreateCollectionReq.StructFieldSchema.builder().name("metadata").build();

        ParamException exception = Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("sub")
                        .dataType(io.milvus.v2.common.DataType.Struct)
                        .build()));
        Assertions.assertTrue(exception.getMessage().contains("does not support Struct as a sub-field"),
                "direct Struct sub-field should be rejected: " + exception.getMessage());
    }

    @Test
    void testConvertFieldReqToStructFieldSchemaRejectsDirectStructSubField() {
        AddFieldReq structReq = AddFieldReq.builder()
                .fieldName("clips")
                .dataType(io.milvus.v2.common.DataType.Array)
                .elementType(io.milvus.v2.common.DataType.Struct)
                .maxCapacity(100)
                .addStructField(AddFieldReq.builder()
                        .fieldName("sub")
                        .dataType(io.milvus.v2.common.DataType.Struct)
                        .build())
                .build();
        ParamException exception = Assertions.assertThrows(ParamException.class,
                () -> SchemaUtils.convertFieldReqToStructFieldSchema(structReq));
        Assertions.assertTrue(exception.getMessage().contains("does not support Struct as a sub-field"),
                "direct Struct sub-field should be rejected: " + exception.getMessage());
    }

    @Test
    void testStructFieldSchemaRejectsNonScalarArrayElementType() {
        CreateCollectionReq.StructFieldSchema structField =
                CreateCollectionReq.StructFieldSchema.builder().name("metadata").build();

        for (io.milvus.v2.common.DataType nonScalar : new io.milvus.v2.common.DataType[]{
                io.milvus.v2.common.DataType.JSON,
                io.milvus.v2.common.DataType.Geometry,
                io.milvus.v2.common.DataType.Text,
                io.milvus.v2.common.DataType.FloatVector}) {
            ParamException exception = Assertions.assertThrows(ParamException.class,
                    () -> structField.addField(AddFieldReq.builder()
                            .fieldName("tags")
                            .dataType(io.milvus.v2.common.DataType.Array)
                            .elementType(nonScalar)
                            .maxCapacity(8)
                            .build()));
            Assertions.assertTrue(exception.getMessage().contains("requires a scalar element_type"),
                    "element type " + nonScalar + " should be rejected: " + exception.getMessage());
        }
    }

    @Test
    void testConvertFieldReqToStructFieldSchemaRejectsNonScalarArrayElementType() {
        for (io.milvus.v2.common.DataType nonScalar : new io.milvus.v2.common.DataType[]{
                io.milvus.v2.common.DataType.JSON,
                io.milvus.v2.common.DataType.Geometry,
                io.milvus.v2.common.DataType.FloatVector}) {
            AddFieldReq structReq = AddFieldReq.builder()
                    .fieldName("clips")
                    .dataType(io.milvus.v2.common.DataType.Array)
                    .elementType(io.milvus.v2.common.DataType.Struct)
                    .maxCapacity(100)
                    .addStructField(AddFieldReq.builder()
                            .fieldName("tags")
                            .dataType(io.milvus.v2.common.DataType.Array)
                            .elementType(nonScalar)
                            .maxCapacity(8)
                            .build())
                    .build();
            ParamException exception = Assertions.assertThrows(ParamException.class,
                    () -> SchemaUtils.convertFieldReqToStructFieldSchema(structReq));
            Assertions.assertTrue(exception.getMessage().contains("requires a scalar element_type"),
                    "element type " + nonScalar + " should be rejected: " + exception.getMessage());
        }
    }

    @Test
    void testStructFieldSchemaRejectsElementNullableOnNonArraySubField() {
        CreateCollectionReq.StructFieldSchema structField =
                CreateCollectionReq.StructFieldSchema.builder().name("metadata").build();

        ParamException exception = Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("lang")
                        .dataType(io.milvus.v2.common.DataType.VarChar)
                        .maxLength(16)
                        .elementNullable(Boolean.TRUE)
                        .build()));
        Assertions.assertTrue(exception.getMessage().contains("elementNullable requires an Array sub-field"));
    }

    @Test
    void testStructFieldSchemaRejectsNestedArrayOfArraySubField() {
        CreateCollectionReq.StructFieldSchema structField =
                CreateCollectionReq.StructFieldSchema.builder().name("metadata").build();

        ParamException exception = Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("tags")
                        .dataType(io.milvus.v2.common.DataType.Array)
                        .elementType(io.milvus.v2.common.DataType.Array)
                        .maxCapacity(8)
                        .build()));
        Assertions.assertTrue(exception.getMessage().contains("nested Array of Array"));
    }

    @Test
    void testStructFieldSchemaRejectsForbiddenSubFieldAttributes() {
        CreateCollectionReq.StructFieldSchema structField =
                CreateCollectionReq.StructFieldSchema.builder().name("metadata").build();

        Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("pk").dataType(io.milvus.v2.common.DataType.Int64).isPrimaryKey(Boolean.TRUE).build()));
        Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("part").dataType(io.milvus.v2.common.DataType.VarChar).maxLength(100)
                        .isPartitionKey(Boolean.TRUE).build()));
        Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("cluster").dataType(io.milvus.v2.common.DataType.VarChar).maxLength(100)
                        .isClusteringKey(Boolean.TRUE).build()));
        Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("auto").dataType(io.milvus.v2.common.DataType.Int64).autoID(Boolean.TRUE).build()));
        Assertions.assertThrows(ParamException.class,
                () -> structField.addField(AddFieldReq.builder()
                        .fieldName("def").dataType(io.milvus.v2.common.DataType.Int64).defaultValue(1L).build()));
    }

    @Test
    void testConvertFromGrpcFieldSchemaPreservesIndexParams() {
        FieldSchema rpcSchema = FieldSchema.newBuilder()
                .setName("vector")
                .setDataType(DataType.FloatVector)
                .addIndexParams(KeyValuePair.newBuilder()
                        .setKey("index_type")
                        .setValue("HNSW")
                        .build())
                .addIndexParams(KeyValuePair.newBuilder()
                        .setKey("metric_type")
                        .setValue("L2")
                        .build())
                .addIndexParams(KeyValuePair.newBuilder()
                        .setKey("params")
                        .setValue("{\"M\":16,\"efConstruction\":200}")
                        .build())
                .build();

        CreateCollectionReq.FieldSchema fieldSchema = SchemaUtils.convertFromGrpcFieldSchema(rpcSchema);

        Assertions.assertNotNull(fieldSchema.getIndexes());
        Assertions.assertEquals(1, fieldSchema.getIndexes().size());
        Map<String, Object> index = fieldSchema.getIndexes().get(0);
        Assertions.assertEquals("HNSW", index.get("index_type"));
        Assertions.assertEquals("L2", index.get("metric_type"));
        Map<?, ?> indexParams = (Map<?, ?>) index.get("params");
        Assertions.assertEquals(16L, ((Number) indexParams.get("M")).longValue());
        Assertions.assertEquals(200L, ((Number) indexParams.get("efConstruction")).longValue());
    }

    @Test
    void testConvertFromGrpcFieldSchemaInitializesEmptyIndexes() {
        FieldSchema rpcSchema = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .build();

        CreateCollectionReq.FieldSchema fieldSchema = SchemaUtils.convertFromGrpcFieldSchema(rpcSchema);

        Assertions.assertNotNull(fieldSchema.getIndexes());
        Assertions.assertTrue(fieldSchema.getIndexes().isEmpty());
    }

    @Test
    void testConvertFromGrpcFieldSchemaSkipsMalformedIndexParams() {
        FieldSchema rpcSchema = FieldSchema.newBuilder()
                .setName("vector")
                .setDataType(DataType.FloatVector)
                .addIndexParams(KeyValuePair.newBuilder()
                        .setKey("index_type")
                        .setValue("HNSW")
                        .build())
                .addIndexParams(KeyValuePair.newBuilder()
                        .setKey("params")
                        .setValue("not-a-json-object")
                        .build())
                .build();

        CreateCollectionReq.FieldSchema fieldSchema = SchemaUtils.convertFromGrpcFieldSchema(rpcSchema);

        Assertions.assertNotNull(fieldSchema.getIndexes());
        Assertions.assertEquals(1, fieldSchema.getIndexes().size());
        Map<String, Object> index = fieldSchema.getIndexes().get(0);
        Assertions.assertEquals("HNSW", index.get("index_type"));
        Assertions.assertFalse(index.containsKey("params"));
    }

    @Test
    void testConvertFromGrpcFieldSchema() {
        CreateCollectionReq.CollectionSchema collectionSchema = buildSchema();
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = collectionSchema.getFieldSchemaList();
        for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
            FieldSchema rpcSchema = SchemaUtils.convertToGrpcFieldSchema(fieldSchema);

            CreateCollectionReq.FieldSchema newSchema = SchemaUtils.convertFromGrpcFieldSchema(rpcSchema);
            Assertions.assertEquals(newSchema.getName(), fieldSchema.getName());
            Assertions.assertEquals(newSchema.getDescription(), fieldSchema.getDescription());
            Assertions.assertEquals(newSchema.getDataType(), fieldSchema.getDataType());
            if (rpcSchema.getDataType() == DataType.Array) {
                Assertions.assertEquals(newSchema.getElementType(), fieldSchema.getElementType());
            }

            Map<String, String> originParams = fieldSchema.getTypeParams();
            if (originParams != null) {
                Map<String, String> typeParams = newSchema.getTypeParams();
                originParams.forEach((k, v) -> {
                    Assertions.assertTrue(typeParams.containsKey(k));
                    Assertions.assertEquals(typeParams.get(k), originParams.get(k));
                });
            }

            Assertions.assertEquals(newSchema.getIsPrimaryKey(), fieldSchema.getIsPrimaryKey());
            Assertions.assertEquals(newSchema.getAutoID(), fieldSchema.getAutoID());
            Assertions.assertEquals(newSchema.getIsPartitionKey(), fieldSchema.getIsPartitionKey());
            Assertions.assertEquals(newSchema.getIsClusteringKey(), fieldSchema.getIsClusteringKey());
            Assertions.assertEquals(newSchema.getIsNullable(), fieldSchema.getIsNullable());

            if (rpcSchema.getName().equals("int64_field")) {
                Assertions.assertEquals(newSchema.getDefaultValue(), fieldSchema.getDefaultValue());
            } else {
                Assertions.assertNull(newSchema.getDefaultValue());
            }

            if (rpcSchema.getName().equals("varchar_field")) {
                Assertions.assertTrue(newSchema.getEnableAnalyzer());
                Assertions.assertTrue(newSchema.getEnableMatch());
                Assertions.assertEquals(newSchema.getAnalyzerParams(), fieldSchema.getAnalyzerParams());
                Assertions.assertEquals(newSchema.getMultiAnalyzerParams(), fieldSchema.getMultiAnalyzerParams());
            }
        }
    }

    @Test
    void testGrpcStructFieldSchemaRoundTripPreservesElementNullableAndNestedArray() {
        CreateCollectionReq.StructFieldSchema struct = CreateCollectionReq.StructFieldSchema.builder()
                .name("clips")
                .nullable(true)
                .maxCapacity(100)
                .build();
        struct.addField(AddFieldReq.builder()
                .fieldName("score")
                .dataType(io.milvus.v2.common.DataType.Int64)
                .isNullable(Boolean.FALSE)
                .build());
        struct.addField(AddFieldReq.builder()
                .fieldName("lang")
                .dataType(io.milvus.v2.common.DataType.VarChar)
                .maxLength(16)
                .isNullable(Boolean.TRUE)
                .build());
        struct.addField(AddFieldReq.builder()
                .fieldName("tags")
                .dataType(io.milvus.v2.common.DataType.Array)
                .elementType(io.milvus.v2.common.DataType.VarChar)
                .maxCapacity(8)
                .maxLength(16)
                .isNullable(Boolean.TRUE)
                .elementNullable(Boolean.TRUE)
                .build());

        StructArrayFieldSchema rpc = SchemaUtils.convertToGrpcStructFieldSchema(struct);

        // row-level nullable inherited by all sub-fields
        Assertions.assertTrue(rpc.getNullable());
        Assertions.assertEquals(3, rpc.getFieldsCount());

        // non-nullable scalar sub-field
        FieldSchema score = rpc.getFields(0);
        Assertions.assertEquals(DataType.Array, score.getDataType());
        Assertions.assertEquals(DataType.Int64, score.getElementType());
        Assertions.assertTrue(score.getNullable());
        Assertions.assertFalse(score.getElementNullable());
        Assertions.assertEquals("100", getParam(score.getTypeParamsList(), "max_capacity"));

        // element-nullable scalar sub-field
        FieldSchema lang = rpc.getFields(1);
        Assertions.assertEquals(DataType.Array, lang.getDataType());
        Assertions.assertEquals(DataType.VarChar, lang.getElementType());
        Assertions.assertTrue(lang.getNullable());
        Assertions.assertTrue(lang.getElementNullable());
        Assertions.assertEquals("16", getParam(lang.getTypeParamsList(), "max_length"));

        // nested Array sub-field: recursive type_schema carries inner capacity and leaf nullability
        FieldSchema tags = rpc.getFields(2);
        Assertions.assertEquals(DataType.Array, tags.getDataType());
        Assertions.assertEquals(DataType.Array, tags.getElementType());
        Assertions.assertTrue(tags.getElementNullable());
        Assertions.assertTrue(tags.hasTypeSchema());
        TypeSchema root = tags.getTypeSchema();
        Assertions.assertTrue(root.getNullable());
        Assertions.assertEquals("100", getParam(root.getTypeParamsList(), "max_capacity"));
        Assertions.assertTrue(root.hasArrayElement());
        TypeSchema inner = root.getArrayElement();
        Assertions.assertTrue(inner.getNullable());
        Assertions.assertEquals("8", getParam(inner.getTypeParamsList(), "max_capacity"));
        Assertions.assertTrue(inner.hasArrayElement());
        TypeSchema leaf = inner.getArrayElement();
        Assertions.assertEquals(DataType.VarChar, leaf.getLeafType());
        Assertions.assertTrue(leaf.getNullable());
        Assertions.assertEquals("16", getParam(leaf.getTypeParamsList(), "max_length"));

        // describe-collection round trip
        CreateCollectionReq.StructFieldSchema roundTripped =
                SchemaUtils.convertFromGrpcStructFieldSchema(rpc);
        Assertions.assertTrue(roundTripped.getNullable());
        Assertions.assertEquals(Integer.valueOf(100), roundTripped.getMaxCapacity());
        Assertions.assertEquals(3, roundTripped.getFields().size());

        CreateCollectionReq.FieldSchema scoreRt = roundTripped.getFields().get(0);
        Assertions.assertEquals(io.milvus.v2.common.DataType.Int64, scoreRt.getDataType());
        Assertions.assertFalse(scoreRt.getIsNullable());
        Assertions.assertEquals(0, scoreRt.getMaxCapacity());

        CreateCollectionReq.FieldSchema langRt = roundTripped.getFields().get(1);
        Assertions.assertEquals(io.milvus.v2.common.DataType.VarChar, langRt.getDataType());
        Assertions.assertTrue(langRt.getIsNullable());
        Assertions.assertEquals(Integer.valueOf(16), langRt.getMaxLength());

        CreateCollectionReq.FieldSchema tagsRt = roundTripped.getFields().get(2);
        Assertions.assertEquals(io.milvus.v2.common.DataType.Array, tagsRt.getDataType());
        Assertions.assertEquals(io.milvus.v2.common.DataType.VarChar, tagsRt.getElementType());
        Assertions.assertTrue(tagsRt.getIsNullable());
        Assertions.assertTrue(tagsRt.getElementNullable());
        Assertions.assertEquals(Integer.valueOf(8), tagsRt.getMaxCapacity());
        Assertions.assertEquals(Integer.valueOf(16), tagsRt.getMaxLength());
    }

    @Test
    void testBuildGrpcStructSubFieldSchemaForAddSubFieldPath() {
        CreateCollectionReq.FieldSchema lang = CreateCollectionReq.FieldSchema.builder()
                .name("clips[lang]")
                .dataType(io.milvus.v2.common.DataType.VarChar)
                .maxLength(16)
                .isNullable(Boolean.TRUE)
                .build();

        FieldSchema rpc = SchemaUtils.buildGrpcStructSubFieldSchema(lang, false, null);
        Assertions.assertEquals("clips[lang]", rpc.getName());
        Assertions.assertEquals(DataType.Array, rpc.getDataType());
        Assertions.assertEquals(DataType.VarChar, rpc.getElementType());
        Assertions.assertFalse(rpc.getNullable());
        Assertions.assertTrue(rpc.getElementNullable());
        Assertions.assertEquals("16", getParam(rpc.getTypeParamsList(), "max_length"));
        Assertions.assertNull(getParam(rpc.getTypeParamsList(), "max_capacity"));
    }

    @Test
    void testBuildGrpcStructSubFieldSchemaPreservesTypeParams() {
        CreateCollectionReq.FieldSchema emb = CreateCollectionReq.FieldSchema.builder()
                .name("clips[emb]")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(128)
                .isNullable(Boolean.TRUE)
                .typeParams(Collections.singletonMap("mmap.enabled", "true"))
                .build();

        FieldSchema rpc = SchemaUtils.buildGrpcStructSubFieldSchema(emb, false, null);
        Assertions.assertEquals(DataType.ArrayOfVector, rpc.getDataType());
        Assertions.assertEquals(DataType.FloatVector, rpc.getElementType());
        // custom typeParams on the sub-field must survive the wrapper conversion
        Assertions.assertEquals("true", getParam(rpc.getTypeParamsList(), "mmap.enabled"));
        Assertions.assertEquals("128", getParam(rpc.getTypeParamsList(), "dim"));
    }

    private static String getParam(List<KeyValuePair> params, String key) {
        for (KeyValuePair kv : params) {
            if (kv.getKey().equals(key)) {
                return kv.getValue();
            }
        }
        return null;
    }
}

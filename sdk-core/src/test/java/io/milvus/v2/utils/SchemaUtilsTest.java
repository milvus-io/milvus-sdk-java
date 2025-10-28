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

package io.milvus.v2.utils;

import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

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
            Assertions.assertEquals(func.getFunctionType(), io.milvus.common.clientenum.FunctionType.fromName(type.name()));
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
}

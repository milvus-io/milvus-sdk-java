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

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.ConsistencyLevel;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.FunctionSchema;
import io.milvus.grpc.FunctionType;
import io.milvus.grpc.StructArrayFieldSchema;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConvertUtilsTest {
    @Test
    void testConvertDescCollectionRespFieldNamesIncludeStructFields() {
        FieldSchema idField = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .setIsPrimaryKey(true)
                .build();

        FieldSchema vectorField = FieldSchema.newBuilder()
                .setName("vector")
                .setDataType(DataType.FloatVector)
                .build();

        CreateCollectionReq.StructFieldSchema structFieldSchema = CreateCollectionReq.StructFieldSchema.builder()
                .name("clips")
                .maxCapacity(10)
                .build();
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("vec")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(8)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("bin_vec")
                .dataType(io.milvus.v2.common.DataType.BinaryVector)
                .dimension(64)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("f16_vec")
                .dataType(io.milvus.v2.common.DataType.Float16Vector)
                .dimension(16)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("bf16_vec")
                .dataType(io.milvus.v2.common.DataType.BFloat16Vector)
                .dimension(16)
                .build());
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("i8_vec")
                .dataType(io.milvus.v2.common.DataType.Int8Vector)
                .dimension(16)
                .build());

        StructArrayFieldSchema rpcStructFieldSchema = SchemaUtils.convertToGrpcStructFieldSchema(structFieldSchema);

        CollectionSchema schema = CollectionSchema.newBuilder()
                .setEnableDynamicField(false)
                .addFields(idField)
                .addFields(vectorField)
                .addStructArrayFields(rpcStructFieldSchema)
                .build();

        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setCollectionName("test")
                .setCollectionID(1L)
                .setDbName("default")
                .setSchema(schema)
                .setNumPartitions(1)
                .setCreatedTimestamp(0L)
                .setCreatedUtcTimestamp(0L)
                .setConsistencyLevel(ConsistencyLevel.Bounded)
                .setShardsNum(1)
                .build();

        DescribeCollectionResp resp = new ConvertUtils().convertDescCollectionResp(response);
        Assertions.assertTrue(resp.getFieldNames().contains("id"));
        Assertions.assertTrue(resp.getFieldNames().contains("vector"));
        Assertions.assertTrue(resp.getFieldNames().contains("clips"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("vector"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[bin_vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[f16_vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[bf16_vec]"));
        Assertions.assertTrue(resp.getVectorFieldNames().contains("clips[i8_vec]"));
    }

    @Test
    void testConvertDescCollectionRespExposesAliasesUpdateTimestampAndSchemaIds() {
        FieldSchema idField = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .setIsPrimaryKey(true)
                .setFieldID(1L)
                .build();
        FieldSchema embeddingField = FieldSchema.newBuilder()
                .setName("embedding")
                .setDataType(DataType.FloatVector)
                .setFieldID(2L)
                .setIsFunctionOutput(true)
                .build();
        FieldSchema dynamicField = FieldSchema.newBuilder()
                .setName("$meta")
                .setDataType(DataType.JSON)
                .setIsDynamic(true)
                .setFieldID(3L)
                .build();
        FunctionSchema functionSchema = FunctionSchema.newBuilder()
                .setName("bm25")
                .setType(FunctionType.BM25)
                .setId(7L)
                .addInputFieldIds(1L)
                .addOutputFieldIds(2L)
                .build();
        CollectionSchema schema = CollectionSchema.newBuilder()
                .setEnableDynamicField(true)
                .setEnableNamespace(true)
                .setVersion(42)
                .addFields(idField)
                .addFields(embeddingField)
                .addFields(dynamicField)
                .addFunctions(functionSchema)
                .build();

        DescribeCollectionResponse response = DescribeCollectionResponse.newBuilder()
                .setCollectionName("test")
                .setCollectionID(1L)
                .setDbName("default")
                .setSchema(schema)
                .setNumPartitions(1)
                .setCreatedTimestamp(0L)
                .setCreatedUtcTimestamp(0L)
                .setConsistencyLevel(ConsistencyLevel.Bounded)
                .setShardsNum(1)
                .addAliases("test_alias")
                .setUpdateTimestamp(123456L)
                .build();

        DescribeCollectionResp resp = new ConvertUtils().convertDescCollectionResp(response);

        Assertions.assertEquals(java.util.Collections.singletonList("test_alias"), resp.getAliases());
        Assertions.assertEquals(123456L, resp.getUpdateTimestamp());
        Assertions.assertEquals(Boolean.TRUE, resp.getEnableNamespace());
        Assertions.assertEquals(42, resp.getSchemaVersion());

        CreateCollectionReq.FieldSchema idFieldResp =
                resp.getCollectionSchema().getFieldSchemaList().stream()
                        .filter(f -> f.getName().equals("id")).findFirst().orElseThrow();
        Assertions.assertEquals(1L, idFieldResp.getFieldId());
        CreateCollectionReq.FieldSchema dynamicFieldResp =
                resp.getCollectionSchema().getFieldSchemaList().stream()
                        .filter(f -> f.getName().equals("$meta")).findFirst().orElseThrow();
        Assertions.assertEquals(Boolean.TRUE, dynamicFieldResp.getIsDynamic());
        CreateCollectionReq.FieldSchema embeddingFieldResp =
                resp.getCollectionSchema().getFieldSchemaList().stream()
                        .filter(f -> f.getName().equals("embedding")).findFirst().orElseThrow();
        Assertions.assertEquals(Boolean.TRUE, embeddingFieldResp.getIsFunctionOutput());

        CreateCollectionReq.Function functionResp = resp.getCollectionSchema().getFunctionList().get(0);
        Assertions.assertEquals(7L, functionResp.getId());
        Assertions.assertEquals(java.util.Collections.singletonList(1L), functionResp.getInputFieldIds());
        Assertions.assertEquals(java.util.Collections.singletonList(2L), functionResp.getOutputFieldIds());
    }
}

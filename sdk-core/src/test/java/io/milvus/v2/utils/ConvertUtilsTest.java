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
import io.milvus.grpc.StructArrayFieldSchema;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

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
    }

    @Test
    public void testStructFieldTypeParamsRoundTrip() {
        // builder: typeParam(key, value). The server reads mmap/warmup from sub-field
        // FieldSchema.type_params; struct-level type_params are persisted and round-tripped.
        CreateCollectionReq.StructFieldSchema structFieldSchema = CreateCollectionReq.StructFieldSchema.builder()
                .name("clips")
                .maxCapacity(10)
                .typeParam("mmap.enabled", "true")
                .typeParam("warmup", "true")
                .build();
        structFieldSchema.addField(AddFieldReq.builder()
                .fieldName("vec")
                .dataType(io.milvus.v2.common.DataType.FloatVector)
                .dimension(8)
                .build());

        StructArrayFieldSchema rpcStructFieldSchema = SchemaUtils.convertToGrpcStructFieldSchema(structFieldSchema);

        // struct-level type_params (mmap.enabled/warmup) reach the proto wire
        Assertions.assertEquals(2, rpcStructFieldSchema.getTypeParamsCount());
        Assertions.assertEquals("true",
                rpcStructFieldSchema.getTypeParamsList().stream()
                        .filter(kv -> kv.getKey().equals("mmap.enabled")).findFirst().get().getValue());
        Assertions.assertEquals("true",
                rpcStructFieldSchema.getTypeParamsList().stream()
                        .filter(kv -> kv.getKey().equals("warmup")).findFirst().get().getValue());

        // describe round-trip reads them back into the V2 struct field schema
        CreateCollectionReq.StructFieldSchema back = SchemaUtils.convertFromGrpcStructFieldSchema(rpcStructFieldSchema);
        Assertions.assertEquals("true", back.getTypeParams().get("mmap.enabled"));
        Assertions.assertEquals("true", back.getTypeParams().get("warmup"));

        // builder: typeParams(map) and setter: setTypeParams(map)
        Map<String, String> map = new HashMap<>();
        map.put("mmap.enabled", "false");
        map.put("warmup", "sync");
        CreateCollectionReq.StructFieldSchema viaMap = CreateCollectionReq.StructFieldSchema.builder()
                .name("meta")
                .maxCapacity(5)
                .typeParams(map)
                .build();
        viaMap.setTypeParams(new HashMap<>());
        viaMap.setTypeParams(map);

        StructArrayFieldSchema rpcViaMap = SchemaUtils.convertToGrpcStructFieldSchema(viaMap);
        Assertions.assertEquals(2, rpcViaMap.getTypeParamsCount());
        Assertions.assertEquals("false",
                rpcViaMap.getTypeParamsList().stream()
                        .filter(kv -> kv.getKey().equals("mmap.enabled")).findFirst().get().getValue());
        Assertions.assertEquals("sync",
                rpcViaMap.getTypeParamsList().stream()
                        .filter(kv -> kv.getKey().equals("warmup")).findFirst().get().getValue());
    }
}

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

public class ConvertUtilsTest {
    @Test
    void testConvertDescCollectionRespFieldNamesIncludeStructFields() {
        FieldSchema idField = FieldSchema.newBuilder()
                .setName("id")
                .setDataType(DataType.Int64)
                .setIsPrimaryKey(true)
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
        Assertions.assertTrue(resp.getFieldNames().contains("clips"));
    }
}


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
import io.milvus.grpc.DataType;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.KeyValuePair;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.ArrayList;
import java.util.List;

public class SchemaUtils {
    public static FieldSchema convertToGrpcFieldSchema(CreateCollectionReq.FieldSchema fieldSchema) {
        FieldSchema schema = FieldSchema.newBuilder()
                .setName(fieldSchema.getName())
                .setDescription(fieldSchema.getDescription())
                .setDataType(DataType.valueOf(fieldSchema.getDataType().name()))
                .setIsPrimaryKey(fieldSchema.getIsPrimaryKey())
                .setIsPartitionKey(fieldSchema.getIsPartitionKey())
                .setAutoID(fieldSchema.getAutoID())
                .build();
        if(fieldSchema.getDimension() != null){
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("dim").setValue(String.valueOf(fieldSchema.getDimension())).build()).build();
        }
//        if (Objects.equals(fieldSchema.getName(), partitionKeyField)) {
//            schema = schema.toBuilder().setIsPartitionKey(Boolean.TRUE).build();
//        }
        if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null){
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(fieldSchema.getMaxLength())).build()).build();
        }
        if (fieldSchema.getDataType() == io.milvus.v2.common.DataType.Array) {
            schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_capacity").setValue(String.valueOf(fieldSchema.getMaxCapacity())).build()).build();
            schema = schema.toBuilder().setElementType(DataType.valueOf(fieldSchema.getElementType().name())).build();
            if (fieldSchema.getElementType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null) {
                schema = schema.toBuilder().addTypeParams(KeyValuePair.newBuilder().setKey("max_length").setValue(String.valueOf(fieldSchema.getMaxLength())).build()).build();
            }
        }
        return schema;
    }

    public static CreateCollectionReq.CollectionSchema convertFromGrpcCollectionSchema(CollectionSchema schema) {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        List<CreateCollectionReq.FieldSchema> fieldSchemas = new ArrayList<>();
        for (FieldSchema fieldSchema : schema.getFieldsList()) {
            fieldSchemas.add(convertFromGrpcFieldSchema(fieldSchema));
        }
        collectionSchema.setFieldSchemaList(fieldSchemas);
        return collectionSchema;
    }

    private static CreateCollectionReq.FieldSchema convertFromGrpcFieldSchema(FieldSchema fieldSchema) {
        CreateCollectionReq.FieldSchema schema = CreateCollectionReq.FieldSchema.builder()
                .name(fieldSchema.getName())
                .description(fieldSchema.getDescription())
                .dataType(io.milvus.v2.common.DataType.valueOf(fieldSchema.getDataType().name()))
                .isPrimaryKey(fieldSchema.getIsPrimaryKey())
                .isPartitionKey(fieldSchema.getIsPartitionKey())
                .autoID(fieldSchema.getAutoID())
                .elementType(io.milvus.v2.common.DataType.valueOf(fieldSchema.getElementType().name()))
                .build();
        for (KeyValuePair keyValuePair : fieldSchema.getTypeParamsList()) {
            if(keyValuePair.getKey().equals("dim")){
                schema.setDimension(Integer.parseInt(keyValuePair.getValue()));
            } else if(keyValuePair.getKey().equals("max_length")){
                schema.setMaxLength(Integer.parseInt(keyValuePair.getValue()));
            } else if(keyValuePair.getKey().equals("max_capacity")){
                schema.setMaxCapacity(Integer.parseInt(keyValuePair.getValue()));
            }
        }
        return schema;
    }
}

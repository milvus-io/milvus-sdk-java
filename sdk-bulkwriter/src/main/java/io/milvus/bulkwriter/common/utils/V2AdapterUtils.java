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

package io.milvus.bulkwriter.common.utils;

import io.milvus.grpc.DataType;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.util.ArrayList;
import java.util.List;

public class V2AdapterUtils {
//    public static CollectionSchemaParam convertV2Schema(CreateCollectionReq.CollectionSchema schemaV2) {
//        CollectionSchemaParam.Builder schemaBuilder = CollectionSchemaParam.newBuilder()
//                .withEnableDynamicField(schemaV2.isEnableDynamicField());
//
//        List<CreateCollectionReq.FieldSchema> fieldSchemaList = schemaV2.getFieldSchemaList();
//        for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
//            FieldType.Builder fieldBuilder = FieldType.newBuilder()
//                    .withName(fieldSchema.getName())
//                    .withDescription(fieldSchema.getDescription())
//                    .withDataType(DataType.valueOf(fieldSchema.getDataType().name()))
//                    .withPrimaryKey(fieldSchema.getIsPrimaryKey())
//                    .withPartitionKey(fieldSchema.getIsPartitionKey())
//                    .withClusteringKey(fieldSchema.getIsClusteringKey())
//                    .withAutoID(fieldSchema.getAutoID());
//            // set vector dimension
//            if(fieldSchema.getDimension() != null){
//                fieldBuilder.withDimension(fieldSchema.getDimension());
//            }
//            // set varchar max length
//            if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null){
//                fieldBuilder.withMaxLength(fieldSchema.getMaxLength());
//            }
//            // set array parameters
//            if (fieldSchema.getDataType() == io.milvus.v2.common.DataType.Array) {
//                fieldBuilder.withMaxCapacity(fieldSchema.getMaxCapacity());
//                fieldBuilder.withElementType(DataType.valueOf(fieldSchema.getElementType().name()));
//                if (fieldSchema.getElementType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null) {
//                    fieldBuilder.withMaxLength(fieldSchema.getMaxLength());
//                }
//            }
//
//            schemaBuilder.addFieldType(fieldBuilder.build());
//        }
//
//        return schemaBuilder.build();
//    }

    private static CreateCollectionReq.FieldSchema convertV1Field(FieldType fieldType) {
        Integer maxLength = fieldType.getMaxLength() > 0 ? fieldType.getMaxLength():65535;
        Integer dimension = fieldType.getDimension() > 0 ? fieldType.getDimension() : null;
        Integer maxCapacity = fieldType.getMaxCapacity() > 0 ? fieldType.getMaxCapacity() : null;
        io.milvus.v2.common.DataType elementType = fieldType.getElementType() == null ? null : io.milvus.v2.common.DataType.valueOf(fieldType.getElementType().name());
        CreateCollectionReq.FieldSchema schemaV2 = CreateCollectionReq.FieldSchema.builder()
                .name(fieldType.getName())
                .description(fieldType.getDescription())
                .dataType(io.milvus.v2.common.DataType.valueOf(fieldType.getDataType().name()))
                .maxLength(maxLength)
                .dimension(dimension)
                .isPrimaryKey(fieldType.isPrimaryKey())
                .isPartitionKey(fieldType.isPartitionKey())
                .isClusteringKey(fieldType.isClusteringKey())
                .autoID(fieldType.isAutoID())
                .elementType(elementType)
                .maxCapacity(maxCapacity)
                .isNullable(fieldType.isNullable())
                .defaultValue(fieldType.getDefaultValue())
                .build();
        return schemaV2;
    }

    public static CreateCollectionReq.CollectionSchema convertV1Schema(CollectionSchemaParam schemaV1) {
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        List<FieldType> fieldTypes = schemaV1.getFieldTypes();
        for (FieldType fieldType : fieldTypes) {
            fieldSchemaList.add(convertV1Field(fieldType));
        }

        return CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(schemaV1.isEnableDynamicField())
                .fieldSchemaList(fieldSchemaList)
                .build();
    }

    public static List<String> getOutputFieldNames(CreateCollectionReq.CollectionSchema schema) {
        List<String> outputFieldNames = new ArrayList<>();
        List<CreateCollectionReq.Function> functionList = schema.getFunctionList();
        for (CreateCollectionReq.Function function : functionList) {
            outputFieldNames.addAll(function.getOutputFieldNames());
        }
        return outputFieldNames;
    }
}

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

import java.util.List;

public class V2AdapterUtils {
    public static CollectionSchemaParam convertV2Schema(CreateCollectionReq.CollectionSchema schemaV2) {
        CollectionSchemaParam.Builder schemaBuilder = CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(schemaV2.isEnableDynamicField());

        List<CreateCollectionReq.FieldSchema> fieldSchemaList = schemaV2.getFieldSchemaList();
        for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
            FieldType.Builder fieldBuilder = FieldType.newBuilder()
                    .withName(fieldSchema.getName())
                    .withDescription(fieldSchema.getDescription())
                    .withDataType(DataType.valueOf(fieldSchema.getDataType().name()))
                    .withPrimaryKey(fieldSchema.getIsPrimaryKey())
                    .withPartitionKey(fieldSchema.getIsPartitionKey())
                    .withClusteringKey(fieldSchema.getIsClusteringKey())
                    .withAutoID(fieldSchema.getAutoID());
            // set vector dimension
            if(fieldSchema.getDimension() != null){
                fieldBuilder.withDimension(fieldSchema.getDimension());
            }
            // set varchar max length
            if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null){
                fieldBuilder.withMaxLength(fieldSchema.getMaxLength());
            }
            // set array parameters
            if (fieldSchema.getDataType() == io.milvus.v2.common.DataType.Array) {
                fieldBuilder.withMaxCapacity(fieldSchema.getMaxCapacity());
                fieldBuilder.withElementType(DataType.valueOf(fieldSchema.getElementType().name()));
                if (fieldSchema.getElementType() == io.milvus.v2.common.DataType.VarChar && fieldSchema.getMaxLength() != null) {
                    fieldBuilder.withMaxLength(fieldSchema.getMaxLength());
                }
            }

            schemaBuilder.addFieldType(fieldBuilder.build());
        }

        return schemaBuilder.build();
    }
}

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

import com.google.gson.*;
import io.milvus.grpc.*;
import io.milvus.param.Constant;
import io.milvus.param.ParamUtils;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;

public class DataUtils {

    public static class InsertBuilderWrapper {
        private InsertRequest.Builder insertBuilder;
        private UpsertRequest.Builder upsertBuilder;

        public InsertRequest convertGrpcInsertRequest(@NonNull InsertReq requestParam,
                                                      DescribeCollectionResp descColl) {
            String collectionName = requestParam.getCollectionName();

            // generate insert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            insertBuilder = InsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getData().size());
            upsertBuilder = null;
            fillFieldsData(requestParam, descColl);
            return insertBuilder.build();
        }

        public UpsertRequest convertGrpcUpsertRequest(@NonNull UpsertReq requestParam,
                                                      DescribeCollectionResp descColl) {
            String collectionName = requestParam.getCollectionName();

            // generate upsert request builder
            MsgBase msgBase = MsgBase.newBuilder().setMsgType(MsgType.Insert).build();
            upsertBuilder = UpsertRequest.newBuilder()
                    .setCollectionName(collectionName)
                    .setBase(msgBase)
                    .setNumRows(requestParam.getData().size());
            insertBuilder = null;
            fillFieldsData(requestParam, descColl);
            return upsertBuilder.build();
        }

        private void addFieldsData(io.milvus.grpc.FieldData value) {
            if (insertBuilder != null) {
                insertBuilder.addFieldsData(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.addFieldsData(value);
            }
        }

        private void setPartitionName(String value) {
            if (insertBuilder != null) {
                insertBuilder.setPartitionName(value);
            } else if (upsertBuilder != null) {
                upsertBuilder.setPartitionName(value);
            }
        }

        private static boolean hasPartitionKey(DescribeCollectionResp descColl) {
            CreateCollectionReq.CollectionSchema collectionSchema = descColl.getCollectionSchema();
            List<CreateCollectionReq.FieldSchema> fieldsList = collectionSchema.getFieldSchemaList();
            for (CreateCollectionReq.FieldSchema field : fieldsList) {
                if (field.getIsPartitionKey() == Boolean.TRUE) {
                    return true;
                }
            }
            return false;
        }

        private void fillFieldsData(UpsertReq requestParam, DescribeCollectionResp descColl) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            if (hasPartitionKey(descColl)) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = "Collection " + requestParam.getCollectionName() + " has partition key, not allow to specify partition name";
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(descColl, rowFields);
        }

        private void fillFieldsData(InsertReq requestParam, DescribeCollectionResp descColl) {
            // set partition name only when there is no partition key field
            String partitionName = requestParam.getPartitionName();
            if (hasPartitionKey(descColl)) {
                if (partitionName != null && !partitionName.isEmpty()) {
                    String msg = "Collection " + requestParam.getCollectionName() + " has partition key, not allow to specify partition name";
                    throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                }
            } else if (partitionName != null) {
                this.setPartitionName(partitionName);
            }

            // convert insert data
            List<JsonObject> rowFields = requestParam.getData();
            checkAndSetRowData(descColl, rowFields);
        }

        private void checkAndSetRowData(DescribeCollectionResp descColl, List<JsonObject> rows) {
            CreateCollectionReq.CollectionSchema collectionSchema = descColl.getCollectionSchema();
            List<CreateCollectionReq.Function> functionsList = collectionSchema.getFunctionList();
            List<String> outputFieldNames = new ArrayList<>();
            for (CreateCollectionReq.Function function : functionsList) {
                outputFieldNames.addAll(function.getOutputFieldNames());
            }

            List<CreateCollectionReq.FieldSchema> fieldsList = collectionSchema.getFieldSchemaList();
            Map<String, InsertDataInfo> nameInsertInfo = new HashMap<>();
            InsertDataInfo insertDynamicDataInfo = InsertDataInfo.builder().field(
                            CreateCollectionReq.FieldSchema.builder()
                                    .name(Constant.DYNAMIC_FIELD_NAME)
                                    .dataType(io.milvus.v2.common.DataType.JSON)
                                    .build())
                    .data(new LinkedList<>()).build();
            for (JsonObject row : rows) {
                for (CreateCollectionReq.FieldSchema field : fieldsList) {
                    String fieldName = field.getName();
                    InsertDataInfo insertDataInfo = nameInsertInfo.getOrDefault(fieldName, InsertDataInfo.builder()
                            .field(field).data(new LinkedList<>()).build());

                    // check normalField
                    JsonElement rowFieldData = row.get(fieldName);
                    if (rowFieldData == null) {
                        // if the field is auto-id, no need to provide value
                        if (field.getAutoID() == Boolean.TRUE) {
                            continue;
                        }

                        // if the field is an output field of doc-in-doc-out, no need to provide value
                        if (outputFieldNames.contains(field.getName())) {
                            continue;
                        }

                        // if the field doesn't have default value, require user provide the value
                        if (!field.getIsNullable() && field.getDefaultValue() == null) {
                            String msg = String.format("The field: %s is not provided.", field.getName());
                            throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                        }

                        rowFieldData = JsonNull.INSTANCE;
                    }

                    // from v2.4.10, milvus allows upsert for auto-id pk, no need to check for upsert action
                    if (field.getAutoID() == Boolean.TRUE && insertBuilder != null) {
                        String msg = String.format("The primary key: %s is auto generated, no need to input.", fieldName);
                        throw new MilvusClientException(ErrorCode.INVALID_PARAMS, msg);
                    }

                    // here we convert the v2 FieldSchema to grpc.FieldSchema then to v1 FieldType
                    // the reason is the logic in ParamUtils.checkFieldValue is complicated, we don't intend to
                    // write duplicate code here
                    FieldSchema grpcField = SchemaUtils.convertToGrpcFieldSchema(field);
                    Object fieldValue = ParamUtils.checkFieldValue(ParamUtils.ConvertField(grpcField), rowFieldData);
                    insertDataInfo.getData().add(fieldValue);
                    nameInsertInfo.put(fieldName, insertDataInfo);
                }

                // deal with dynamicField
                if (collectionSchema.isEnableDynamicField()) {
                    JsonObject dynamicField = new JsonObject();
                    for (String rowFieldName : row.keySet()) {
                        if (!nameInsertInfo.containsKey(rowFieldName)) {
                            dynamicField.add(rowFieldName, row.get(rowFieldName));
                        }
                    }
                    insertDynamicDataInfo.getData().add(dynamicField);
                }
            }

            for (String fieldNameKey : nameInsertInfo.keySet()) {
                InsertDataInfo insertDataInfo = nameInsertInfo.get(fieldNameKey);
                // here we convert the v2 FieldSchema to grpc.FieldSchema then to v1 FieldType
                // the reason is the logic in ParamUtils.genFieldData is complicated, we don't intend to
                // write duplicate code here
                FieldSchema grpcField = SchemaUtils.convertToGrpcFieldSchema(insertDataInfo.getField());
                this.addFieldsData(ParamUtils.genFieldData(ParamUtils.ConvertField(grpcField), insertDataInfo.getData()));
            }
            if (collectionSchema.isEnableDynamicField()) {
                // here we convert the v2 FieldSchema to grpc.FieldSchema then to v1 FieldType
                // the reason is the logic in ParamUtils.genFieldData is complicated, we don't intend to
                // write duplicate code here
                FieldSchema grpcField = SchemaUtils.convertToGrpcFieldSchema(insertDynamicDataInfo.getField());
                this.addFieldsData(ParamUtils.genFieldData(ParamUtils.ConvertField(grpcField), insertDynamicDataInfo.getData(), Boolean.TRUE));
            }
        }
    }

    @Builder
    @Getter
    public static class InsertDataInfo {
        private final CreateCollectionReq.FieldSchema field;
        private final LinkedList<Object> data;
    }
}
